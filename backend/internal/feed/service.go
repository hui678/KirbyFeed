package feed

import (
	"context"
	"encoding/json"
	"errors"
	"feedsystem_video_go/internal/dbguard"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/storage"
	"feedsystem_video_go/internal/video"
	"fmt"
	"log"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/patrickmn/go-cache"
	redis "github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

// singleRedisOpTimeout 是单个 Redis 操作的超时预算。
//
// 带 timeout 的 context 的额度会被串联在其上的所有 IO 操作连续消耗。
// 因此每个 Redis 操作都必须各自新建 context，不能把同一个 context
// 复用到「加锁 -> 查库 -> 回填」这种多步骤流程上，否则后续写入会
// 因预算耗尽而失败（错误被丢弃时表现为缓存永远写不进去）。
const singleRedisOpTimeout = 50 * time.Millisecond

// ---- 热榜（滑动窗口 + 快照稳定分页）----

// hotWindowMinutes 是滑动窗口长度：榜单聚合「最近 60 个分钟桶」，即近一小时热度。
const hotWindowMinutes = 60

// TimelineReconcileInterval 是热时间线的对账周期。
// 只补缺失、不删多余：删除视频走 ZREM 的正常路径；对账期间并发发布的新视频
// 可能不在本次 MySQL 快照里，删「多余」会误伤它们。
const TimelineReconcileInterval = 5 * time.Minute

// followingHeadCacheTTL 是关注流**第一页**的缓存时间。
// 新视频只会插在第一页之前，后续页的游标锚定在旧数据上，发布不会影响它们，
// 所以只有首页需要短 TTL。旧实现所有页统一 24h，导致发布后最长一天都刷不到新视频。
const followingHeadCacheTTL = 30 * time.Second

// hotAsOfLookback 是 as_of 允许回溯的最长时间。
// 它同时是「一个分页会话最长能活多久」的上限：超出后 as_of 已滑出可翻页范围，
// 继续翻页只能明确终止，否则客户端会拿到错位的数据。
const hotAsOfLookback = (hotWindowMinutes - 1) * time.Minute

// hotSnapshotTTL 是榜单快照的存活时间。
// 它现在只控制两件事：同一个 as_of 多久重建一次、榜单最多滞后多久。
// 「重建结果与首份一致」不再依赖 TTL 的长短——as_of 只取已封桶的分钟，
// 60 个输入桶在生成时就已经定稿（见 hotLatestSealedMinute）。
const hotSnapshotTTL = 2 * time.Minute

// hotLatestSealedMinute 返回「最后一个已经封桶的分钟」。
//
// 为什么 as_of 不能取当前分钟：当前分钟桶还在被写入，此刻生成的快照只包含
// 这一分钟的前半段；等 TTL 过期后重建，桶里已经多出后半段的事件，同一 as_of
// 的榜单就变了——翻页会重复或漏项。取上一个完整分钟，60 个桶在生成瞬间全部
// 定稿，重建才真的与首份逐位一致。
func hotLatestSealedMinute(now time.Time) time.Time {
	return now.UTC().Truncate(time.Minute).Add(-time.Minute)
}

// hotSnapshotCap 是合并后快照保留的最大条目数。
// 不裁剪时 M（快照规模）=「过去一小时有过任何热度事件的视频数」，百万级视频下会
// 膨胀到百万量级：单次合并的 O(M·log M) 排序要数秒，快照本身也要上百 MB，
// 而榜单实际只会露出前几十条。裁剪把 M 的上界压成常数。
// 代价：榜单深度被截断到 Top-N，再往后翻不到 —— 对只展示头部的榜单入口可接受。
const hotSnapshotCap = 5000

// hotMergeTimeout 是 ZUNIONSTORE 的专用超时预算。
// 它是全链路最贵的一步（O(N) + O(M·log M)，N 为 60 个分钟桶的元素总数），必须和
// Exists / Expire / ZRevRange 这些 O(1)~O(log N) 的操作分开给预算，否则额度会被
// 前面的小操作吃掉，表现为「合并永远超时、快照永远写不进去」。
const hotMergeTimeout = 500 * time.Millisecond

// maxConcurrentDBLoad 的容量不再由包级常量决定：dbguard 在 db.Open 里按连接池
// 上限的一半配置（默认 64 → 32），调大 MYSQL_MAX_OPEN_CONNS 时闸门会同步放大。
// 详见 internal/dbguard 与 config.DatabaseConfig 的注释。

// errDBOverloaded 表示回源并发已达上限，本次请求被主动丢弃以保护数据库。
// 它包装 apierror.ErrServiceOverloaded，HTTP 层会把它分类成 503 而不是 500 ——
// 这是「主动限流降级」而不是「服务故障」，客户端收到 503 才知道该退避重试。
var errDBOverloaded = dbguard.ErrOverloaded

type FeedService struct {
	repo         *FeedRepository
	likeRepo     *video.LikeRepository
	rediscache   *rediscache.Client
	localcache   *cache.Cache
	cacheTTL     time.Duration
	requestGroup singleflight.Group
	storage      storage.Storage
}

type CachedFeedData struct {
	PublicVideos []video.Video `json:"public_videos"`
}

// CachedFollowingPage 是关注流的列表缓存内容：**只存 ID 与游标**，不存渲染结果。
//
// 存整页结果时，`IsLiked`、`LikesCount`、标题、删除状态全部被冻结 24 小时；
// 改成只存 ID 后，每次命中都重新走实体缓存 + liked 查询，动态字段永远是最新的。
// 这是「失效粒度从全站变成单个实体」的结构性修法，代价是多一次 MGET。
type CachedFollowingPage struct {
	VideoIDs     []uint `json:"video_ids"`
	HasMore      bool   `json:"has_more"`
	NextTime     int64  `json:"next_time"`
	NextIDBefore *uint  `json:"next_id_before,omitempty"`
}

func NewFeedService(repo *FeedRepository, likeRepo *video.LikeRepository, rediscache *rediscache.Client, store storage.Storage) *FeedService {
	return &FeedService{
		repo:       repo,
		likeRepo:   likeRepo,
		rediscache: rediscache,
		localcache: cache.New(3*time.Second, 5*time.Second),
		cacheTTL:   24 * time.Hour,
		storage:    store,
	}
}

func (f *FeedService) GetVideoByIDs(ctx context.Context, videoIDs []uint) ([]*video.Video, error) {
	// GetVideoByIDs 批量获取视频信息
	// 采用 L1(本地缓存) -> L2(Redis) -> L3(MySQL) 三级架构
	if len(videoIDs) == 0 {
		return []*video.Video{}, nil
	}

	videoMap := make(map[uint]*video.Video)
	//L1:本地缓存
	var missedL1 []uint
	for _, id := range videoIDs {
		cacheKey := video.VideoEntityKey(f.rediscache, id)
		if f.localcache != nil {
			if v, found := f.localcache.Get(cacheKey); found {
				if data, ok := v.(video.Video); ok {
					videoMap[id] = &data
					continue
				}
			}
		}
		// 记录未命中的 ID，准备进入下一级缓存
		missedL1 = append(missedL1, id)
	}

	if len(missedL1) == 0 {
		return buildOrderedResult(videoIDs, videoMap), nil
	}

	//L2:redis
	var missedL2 []uint
	redisAvailable := true
	if len(missedL1) > 0 {
		cacheKeys := make([]string, len(missedL1))
		for i, id := range missedL1 {
			cacheKeys[i] = video.VideoEntityKey(f.rediscache, id)
		}

		cacheCtx, cancel := context.WithTimeout(ctx, singleRedisOpTimeout)
		results, err := f.rediscache.MGet(cacheCtx, cacheKeys...)
		cancel()

		if err == nil {
			for i, res := range results {
				id := missedL1[i]
				if res != nil {
					if str, ok := res.(string); ok {
						// 占位值：缓存明确记录了「这个 ID 不存在」，直接跳过，不再回源
						if video.IsVideoEntityNull(str) {
							continue
						}
						var v video.Video
						if err := json.Unmarshal([]byte(str), &v); err == nil {
							videoMap[id] = &v
							// 回写更新 L1 本地缓存
							if f.localcache != nil {
								f.localcache.Set(cacheKeys[i], v, 5*time.Second)
							}
							continue
						}
					}
				}
				missedL2 = append(missedL2, id)
			}
		} else {
			// 如果 Redis 挂了或者超时了，全部降级到 L3
			missedL2 = missedL1
			redisAvailable = false
			log.Printf("L2 Redis MGet 失败，全部降级到 MySQL: %v", err)
		}
	}

	if len(missedL2) == 0 {
		return buildOrderedResult(videoIDs, videoMap), nil
	}

	// 回写基准必须在查库之前拿到：失效可能发生在「查库之后、回写之前」，
	// 那种情况下查回的是旧值，CAS 必须失败。一次 MGET 批量取版本，
	// 而不是每个 ID 各发一次 GET。
	versions := make(map[uint]string, len(missedL2))
	versionsOK := redisAvailable
	if versionsOK && len(missedL2) > 0 {
		versionKeys := make([]string, len(missedL2))
		for i, id := range missedL2 {
			versionKeys[i] = video.VideoEntityVersionKey(f.rediscache, id)
		}
		verCtx, verCancel := context.WithTimeout(ctx, singleRedisOpTimeout)
		verResults, verErr := f.rediscache.MGet(verCtx, versionKeys...)
		verCancel()
		if verErr != nil {
			versionsOK = false
			log.Printf("读取实体缓存版本号失败，本次不回写: %v", verErr)
		} else {
			for i, res := range verResults {
				if s, ok := res.(string); ok {
					versions[missedL2[i]] = s
				}
			}
		}
	}

	//L3:MySQL
	var wg sync.WaitGroup
	var mu sync.Mutex
	var failed int
	var shed int
	var firstErr error
	for _, id := range missedL2 {
		wg.Add(1)
		go func(videoID uint) {
			defer wg.Done()
			// 与详情页 GetDetail 共用同一个回源去重组：两条路径命中的是同一个
			// video:entity:<id>，只有共用 Group 与 key，并发回源才能真正合并。
			v, err, _ := video.EntityLoadGroup.Do(video.EntitySingleflightKey(videoID), func() (interface{}, error) {
				// Redis 宕机全量降级时，这个闸门是唯一挡在 MySQL 前面的东西。
				// 拿不到配额就返回 503，而不是静默丢掉这一条 —— 短页会被客户端
				// 误读成「后面没有内容了」。
				var videoList []*video.Video
				if err := dbguard.Do(func() error {
					var e error
					videoList, e = f.repo.GetByIDs(ctx, []uint{videoID})
					return e
				}); err != nil {
					return nil, err
				}
				if len(videoList) == 0 {
					// 记下「这个 ID 确实不存在」，否则同一个不存在的 ID 每次请求都会
					// 打一次 MySQL —— 这就是缓存穿透。回写不受请求生命周期限制。
					video.ScheduleEntityWrite(func() {
						video.SetVideoEntityNull(context.Background(), f.rediscache, videoID)
					})
					return nil, nil
				}

				safeCopy := *videoList[0]
				if b, err := json.Marshal(safeCopy); err == nil && versionsOK {
					version := versions[videoID]
					video.ScheduleEntityWrite(func() {
						setCtx, setCancel := context.WithTimeout(context.Background(), singleRedisOpTimeout)
						defer setCancel()
						video.SetVideoEntityIfVersion(setCtx, f.rediscache, safeCopy.ID, b, video.VideoEntityTTL, version)
					})
				}
				return videoList[0], nil
			})

			if err != nil {
				mu.Lock()
				failed++
				if errors.Is(err, errDBOverloaded) {
					shed++
				}
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}
			if v == nil { // 实体明确不存在（已写哨兵），不出现在结果里
				return
			}

			safeCopy := *(v.(*video.Video))
			mu.Lock()
			videoMap[id] = &safeCopy
			mu.Unlock()
			if f.localcache != nil {
				f.localcache.Set(video.VideoEntityKey(f.rediscache, safeCopy.ID), safeCopy, 5*time.Second)
			}
		}(id)
	}
	wg.Wait()

	result := buildOrderedResult(videoIDs, videoMap)
	// 有失败就往上抛：返回「短页 + nil error」会让客户端把数据库故障当成
	// 「到底了」，而热榜还会按 member 数推进 offset，把缺失的条目永久跳过。
	if firstErr != nil {
		return result, fmt.Errorf("feed: hydrate %d/%d videos from DB failed (gate shed %d): %w",
			failed, len(missedL2), shed, firstErr)
	}
	return result, nil
}

// 查询最新视频 (冷热分离 + 复合游标分页)
//
// 全链路的排序键统一为 (create_time DESC, id DESC)：MySQL 的 ORDER BY、
// Redis 取回后的应用层重排、以及游标的比较方式，三者必须完全一致，
// 否则冷热两段对同一批数据的先后认知不同，缝合处就会错位。
func (f *FeedService) ListLatest(ctx context.Context, limit int, cursor *TimelineCursor, viewerAccountID uint) (ListLatestResponse, error) {
	if f.rediscache == nil {
		return f.listLatestFromDB(ctx, limit, cursor, viewerAccountID)
	}

	timelineKey := video.TimelineZSetKey(f.rediscache)

	// 获取 ZSET 中最老的一条数据
	zsetTail, err := f.rediscache.ZRangeWithScores(ctx, timelineKey, 0, 0)

	if err != nil {
		return f.listLatestFromDB(ctx, limit, cursor, viewerAccountID)
	}

	if len(zsetTail) == 0 {
		//全局静态锁：无视所有用户的不同游标
		sfKey := f.rediscache.Key("sf:fallback:global_timeline_rebuild")

		v, err, _ := f.requestGroup.Do(sfKey, func() (interface{}, error) {
			// 无视游标，直接去 MySQL 捞最新的 N 条（N 与写入侧裁剪共用同一常量）
			var dbVideos []*video.Video
			if err := dbguard.Do(func() error {
				var e error
				dbVideos, e = f.repo.ListLatest(ctx, video.TimelineCapacity, nil)
				return e
			}); err != nil {
				return nil, err
			}
			if len(dbVideos) == 0 {
				return "EMPTY_DB", nil // 防无限递归
			}

			// 重建 ZSET
			bgCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			var zElements []redis.Z
			for _, vid := range dbVideos {
				zElements = append(zElements, redis.Z{
					Score:  float64(vid.CreateTime.UnixMilli()),
					Member: fmt.Sprintf("%d", vid.ID),
				})
			}
			if err := f.rediscache.ZAdd(bgCtx, timelineKey, zElements...); err != nil {
				return nil, err
			}
			// 集合里可能残留旧成员，重建后统一裁到容量上限，避免重建反而把集合撑大
			if err := f.rediscache.ZRemRangeByRank(bgCtx, timelineKey, 0, -(video.TimelineCapacity + 1)); err != nil {
				log.Printf("timeline rebuild: 裁剪失败: %v", err)
			}
			return "SUCCESS", nil
		})

		if err != nil {
			return ListLatestResponse{}, err
		}
		if v == "EMPTY_DB" {
			return ListLatestResponse{HasMore: false}, nil
		}

		// 让所有被阻塞的请求重新查一遍
		return f.ListLatest(ctx, limit, cursor, viewerAccountID)
	}

	watermark := int64(zsetTail[0].Score)

	// 无游标 = 从最新开始，一定落在热区
	reqTime := time.Now().UnixMilli()
	if cursor != nil {
		reqTime = cursor.CreateTime.UnixMilli()
	}

	// 多取一条用于探测「还有没有下一页」。
	// 旧实现用「返回条数 == limit」推断 has_more，一旦热区里有视频被删除、
	// 装配时被过滤，条数就会永远凑不满，has_more 会提前变成 false。
	want := limit + 1

	var baseVideos []*video.Video

	if cursor != nil && reqTime <= watermark {
		// ===== 冷区：整页走 MySQL =====

		// 针对个别用户的防并发（此时可以用游标做锁，因为冷尾流量极小）
		sfKey := f.rediscache.Key("sf:cold:listLatest:%d:%d:%d", want, reqTime, cursor.ID)
		v, err, _ := f.requestGroup.Do(sfKey, func() (interface{}, error) {
			var videos []*video.Video
			if err := dbguard.Do(func() error {
				var e error
				videos, e = f.repo.ListLatest(ctx, want, cursor)
				return e
			}); err != nil {
				return nil, err
			}
			return videos, nil
		})
		if err != nil {
			return ListLatestResponse{}, err
		}
		baseVideos = v.([]*video.Video)
		// 不回写 ZSET，防止冷数据污染热点时间线

	} else {
		// ===== 热区：整页走 Redis ZSET =====
		videoIDs, err := f.listHotIDs(ctx, timelineKey, want, cursor, reqTime)
		if err != nil {
			return ListLatestResponse{}, err
		}

		if len(videoIDs) > 0 {
			baseVideos, err = f.GetVideoByIDs(ctx, videoIDs)
			if err != nil {
				return ListLatestResponse{}, err
			}
		}

		// 热区不足 want 条（跨过冷热边界，或热区里有视频被删除过滤）：
		// 用热区最后一条的**复合**游标去 MySQL 补齐。
		// MySQL 是 ZSET 的超集，且冷区查询没有任何后置过滤，所以补一次就够，
		// 不需要循环回源——补回来的条数小于请求数，就是真的到底了。
		if len(baseVideos) < want {
			remainLimit := want - len(baseVideos)

			var coldCursor *TimelineCursor
			if len(baseVideos) > 0 {
				last := baseVideos[len(baseVideos)-1]
				coldCursor = &TimelineCursor{CreateTime: last.CreateTime, ID: last.ID}
			} else {
				// 首页、且热区头部的数据已从 MySQL 删除：游标退化为 nil，
				// 等价于"从头取 MySQL 最新的一页"，不能在这里解引用 nil。
				coldCursor = cursor
			}

			beforeVal := int64(0)
			idVal := uint(0)
			if coldCursor != nil {
				beforeVal = coldCursor.CreateTime.UnixMilli()
				idVal = coldCursor.ID
			}

			sfKey := f.rediscache.Key("sf:stitch:listLatest:%d:%d:%d", remainLimit, beforeVal, idVal)
			v, err, _ := f.requestGroup.Do(sfKey, func() (interface{}, error) {
				var videos []*video.Video
				if err := dbguard.Do(func() error {
					var e error
					videos, e = f.repo.ListLatest(ctx, remainLimit, coldCursor)
					return e
				}); err != nil {
					return nil, err
				}
				return videos, nil
			})
			if err != nil {
				// 不能悄悄返回短页：那会让客户端把「数据库故障」当成「没有更多了」
				return ListLatestResponse{}, err
			}
			baseVideos = append(baseVideos, v.([]*video.Video)...)
		}
	}

	// 多取的一条只用于判断 has_more，不返回给客户端
	hasMore := len(baseVideos) > limit
	if hasMore {
		baseVideos = baseVideos[:limit]
	}

	resp := ListLatestResponse{HasMore: hasMore}
	if len(baseVideos) > 0 {
		// 下一页的游标必须是复合的：只回 create_time 会让同毫秒数据被跳过
		last := baseVideos[len(baseVideos)-1]
		nextID := last.ID
		resp.NextTime = last.CreateTime.UnixMilli()
		resp.NextIDBefore = &nextID
	}

	feedVideos, err := f.buildFeedVideos(ctx, baseVideos, viewerAccountID)
	if err != nil {
		return ListLatestResponse{}, err
	}
	resp.VideoList = feedVideos
	return resp, nil
}

// listHotIDs 从 ZSET 取游标之后的最多 limit 个视频 ID，顺序为 (score DESC, id DESC)。
//
// 三个容易踩的点，逐个对应下面的实现：
//  1. 上界必须取"含 reqTime"，再用 id 过滤。历史上这里取的是 reqTime-1，
//     那会把游标所在那一毫秒的所有视频一并排除 —— 这是漏项的根因。
//  2. 因为改成了含 reqTime，本页要多取"该毫秒上的条数"，否则它们会把名额占满。
//  3. Redis 对同 score 的 member 按**字典序**排（"9" > "10"），不是数值序；
//     所以取出后在应用层按 (score DESC, id DESC) 重排，与 MySQL 的 ORDER BY 对齐。
func (f *FeedService) listHotIDs(ctx context.Context, key string, want int, cursor *TimelineCursor, reqTime int64) ([]uint, error) {
	maxScore := "+inf"
	count := int64(want)

	if cursor != nil {
		maxScore = strconv.FormatInt(reqTime, 10)
		if ties, err := f.rediscache.ZCount(ctx, key, maxScore, maxScore); err == nil && ties > 0 {
			count += ties
		}
	}

	pairs, err := f.rediscache.ZRevRangeByScoreWithScores(ctx, key, maxScore, "-inf", 0, count)
	if err != nil {
		return nil, err
	}

	type timelineEntry struct {
		score int64
		id    uint
	}
	entries := make([]timelineEntry, 0, len(pairs))
	for _, p := range pairs {
		member, ok := p.Member.(string)
		if !ok {
			continue
		}
		id, err := strconv.ParseUint(member, 10, 64)
		if err != nil {
			continue
		}
		score := int64(p.Score)

		// 严格位于游标之后：更早的时间，或同一毫秒但更小的 id
		if cursor != nil {
			if score > reqTime {
				continue
			}
			if score == reqTime && uint(id) >= cursor.ID {
				continue
			}
		}
		entries = append(entries, timelineEntry{score: score, id: uint(id)})
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].score != entries[j].score {
			return entries[i].score > entries[j].score
		}
		return entries[i].id > entries[j].id
	})

	if len(entries) > want {
		entries = entries[:want]
	}

	ids := make([]uint, 0, len(entries))
	for _, e := range entries {
		ids = append(ids, e.id)
	}
	return ids, nil
}

func (f *FeedService) listLatestFromDB(ctx context.Context, limit int, cursor *TimelineCursor, viewerAccountID uint) (ListLatestResponse, error) {
	// 与 Redis 路径同样的探测方式：多取一条判断是否还有下一页
	var videos []*video.Video
	if err := dbguard.Do(func() error {
		var e error
		videos, e = f.repo.ListLatest(ctx, limit+1, cursor)
		return e
	}); err != nil {
		return ListLatestResponse{}, err
	}
	hasMore := len(videos) > limit
	if hasMore {
		videos = videos[:limit]
	}
	feedVideos, err := f.buildFeedVideos(ctx, videos, viewerAccountID)
	if err != nil {
		return ListLatestResponse{}, err
	}
	resp := ListLatestResponse{
		VideoList: feedVideos,
		HasMore:   hasMore,
	}
	if len(videos) > 0 {
		last := videos[len(videos)-1]
		nextID := last.ID
		resp.NextTime = last.CreateTime.UnixMilli()
		resp.NextIDBefore = &nextID
	}
	return resp, nil
}

// 按照关注列表查询视频
// ListByFollowing 与 ListLatest 共用同一套复合游标语义。
// 游标单位统一为**毫秒**（历史上这里用的是秒，与 listLatest 不一致，属于契约缺陷）。
func (f *FeedService) ListByFollowing(ctx context.Context, limit int, cursor *TimelineCursor, viewerAccountID uint) (ListByFollowingResponse, error) {
	// 列表缓存只存 ID 与游标；命中和回源都从这里统一装配。
	// 动态字段（IsLiked / LikesCount / 已删除 / 改名）因此不会再被冻结 24 小时。
	buildResponse := func(page CachedFollowingPage, videos []*video.Video) (ListByFollowingResponse, error) {
		feedVideos, err := f.buildFeedVideos(ctx, videos, viewerAccountID)
		if err != nil {
			return ListByFollowingResponse{}, err
		}
		return ListByFollowingResponse{
			VideoList:    feedVideos,
			NextTime:     page.NextTime,
			NextIDBefore: page.NextIDBefore,
			HasMore:      page.HasMore,
		}, nil
	}

	hydrate := func(page CachedFollowingPage) (ListByFollowingResponse, error) {
		videos, err := f.GetVideoByIDs(ctx, page.VideoIDs)
		if err != nil {
			return ListByFollowingResponse{}, err
		}
		return buildResponse(page, videos)
	}

	// 这是 JOIN 关注表 + 排序的昂贵查询，而且 Redis 宕机时 SETNX 锁失效、
	// 所有请求都会直接落到这里 —— 是最需要先挡住的回源路径。
	doListByFollowingFromDB := func() (CachedFollowingPage, []*video.Video, error) {
		// 与 ListLatest 一致：多取一条判断是否还有下一页，
		// 避免「本页刚好被删除/过滤掉几条」时 has_more 提前变成 false。
		var videos []*video.Video
		if err := dbguard.Do(func() error {
			var e error
			videos, e = f.repo.ListByFollowing(ctx, limit+1, viewerAccountID, cursor)
			return e
		}); err != nil {
			return CachedFollowingPage{}, nil, err
		}
		hasMore := len(videos) > limit
		if hasMore {
			videos = videos[:limit]
		}

		page := CachedFollowingPage{
			VideoIDs: make([]uint, 0, len(videos)),
			HasMore:  hasMore,
		}
		for _, v := range videos {
			page.VideoIDs = append(page.VideoIDs, v.ID)
		}
		if len(videos) > 0 {
			last := videos[len(videos)-1]
			nextID := last.ID
			page.NextTime = last.CreateTime.UnixMilli()
			page.NextIDBefore = &nextID
		}
		return page, videos, nil
	}

	// 首页短 TTL、后续页长 TTL：游标翻页是单向的，只有首页会被新视频影响
	cacheTTL := f.cacheTTL
	if cursor == nil {
		cacheTTL = followingHeadCacheTTL
	}
	var cacheKey string
	if viewerAccountID != 0 && f.rediscache != nil {
		before := int64(0)
		idBefore := uint(0)
		if cursor != nil {
			before = cursor.CreateTime.UnixMilli()
			idBefore = cursor.ID
		}
		// 游标进入了 key：关注流的整页缓存必须按完整游标区分，否则不同页会串味。
		// v2 是「只存 ID」的新格式：旧格式的整页响应反序列化到 CachedFollowingPage
		// 不报错但字段全空，不换 key 会让老缓存表现为「关注流忽然空了」。
		cacheKey = f.rediscache.Key("feed:listByFollowing:v2:limit=%d:accountID=%d:before=%d:id=%d", limit, viewerAccountID, before, idBefore)

		// 每个 Redis 操作都要各自申请一份超时预算：同一个 context 的额度会被
		// 串联在上面的操作连续消耗，共用的话最后一步回填必然超时。
		getCached := func() (CachedFollowingPage, bool) {
			opCtx, cancel := context.WithTimeout(ctx, singleRedisOpTimeout)
			defer cancel()

			b, err := f.rediscache.GetBytes(opCtx, cacheKey)
			if err != nil {
				return CachedFollowingPage{}, false
			}
			var cached CachedFollowingPage
			if err := json.Unmarshal(b, &cached); err != nil {
				return CachedFollowingPage{}, false
			}
			return cached, true
		}
		setCached := func(page CachedFollowingPage) {
			b, err := json.Marshal(page)
			if err != nil {
				return
			}
			opCtx, cancel := context.WithTimeout(ctx, singleRedisOpTimeout)
			defer cancel()
			_ = f.rediscache.SetBytes(opCtx, cacheKey, b, cacheTTL)
		}

		readCtx, readCancel := context.WithTimeout(ctx, singleRedisOpTimeout)
		b, err := f.rediscache.GetBytes(readCtx, cacheKey)
		readCancel()

		if err == nil {
			var cached CachedFollowingPage
			if err := json.Unmarshal(b, &cached); err == nil {
				return hydrate(cached)
			}
		} else if rediscache.IsMiss(err) { // 只有 key 明确不存在才回源，连接错误直接兜底查库
			lockKey := "lock:" + cacheKey

			lockCtx, lockCancel := context.WithTimeout(ctx, singleRedisOpTimeout)
			token, locked, lockErr := f.rediscache.Lock(lockCtx, lockKey, 500*time.Millisecond)
			lockCancel()

			if lockErr == nil && locked {
				// 解锁不能受请求生命周期限制，否则客户端断开会导致锁残留
				defer func() { _ = f.rediscache.Unlock(context.Background(), lockKey, token) }()

				if cached, ok := getCached(); ok { // 双重检查：等锁期间可能已被回填
					return hydrate(cached)
				}

				page, videos, err := doListByFollowingFromDB()
				if err != nil {
					return ListByFollowingResponse{}, err
				}
				setCached(page)
				return buildResponse(page, videos)
			}

			// 没抢到锁：轮询等待持锁者回填，同时响应客户端取消
			for i := 0; i < 5; i++ {
				select {
				case <-ctx.Done():
					return ListByFollowingResponse{}, ctx.Err()
				case <-time.After(20 * time.Millisecond):
				}
				if cached, ok := getCached(); ok {
					return hydrate(cached)
				}
			}
		}
	}

	page, videos, err := doListByFollowingFromDB()
	if err != nil {
		return ListByFollowingResponse{}, err
	}
	if cacheKey != "" {
		if b, err := json.Marshal(page); err == nil {
			writeCtx, writeCancel := context.WithTimeout(ctx, singleRedisOpTimeout)
			defer writeCancel()
			_ = f.rediscache.SetBytes(writeCtx, cacheKey, b, cacheTTL)
		}
	}
	return buildResponse(page, videos)
}

// hotSnapshotKey 返回 asOf 时刻对应的榜单快照 key。
// key 里带分钟级 as_of，因此不同分钟的快照互不干扰，也不会出现大批 key 同时失效。
func (f *FeedService) hotSnapshotKey(asOf time.Time) string {
	return f.rediscache.Key("hot:video:merge:1m:%s", asOf.Format("200601021504"))
}

// hotSnapshotEmptyKey 标记「这个 as_of 的窗口确实为空」。
//
// 没有这个标记时，空窗口的合并结果是一个不存在的 key：EXISTS 永远 miss，
// 每个读请求都会重跑一次 60 桶 ZUNIONSTORE。标记的 TTL 与快照一致，
// 因此同一个 as_of 最多每 2 分钟合并一次，而不是每请求一次。
func (f *FeedService) hotSnapshotEmptyKey(asOf time.Time) string {
	return f.rediscache.Key("hot:video:merge:1m:%s:empty", asOf.Format("200601021504"))
}

// hotWindowKeys 返回 [asOf-59min, asOf] 这 60 个分钟桶的 key。
func (f *FeedService) hotWindowKeys(asOf time.Time) []string {
	keys := make([]string, 0, hotWindowMinutes)
	for i := 0; i < hotWindowMinutes; i++ {
		keys = append(keys, f.rediscache.Key("hot:video:1m:%s", asOf.Add(-time.Duration(i)*time.Minute).Format("200601021504")))
	}
	return keys
}

// buildHotSnapshot 合并 60 个分钟桶，生成 asOf 时刻的榜单快照。
//
// asOf 必须是已封桶的分钟（由 hotLatestSealedMinute 保证），因此 60 个输入桶
// 在生成瞬间全部定稿：过期后重建的结果与首份逐位一致，翻页期间排序键不会变化。
// 调用方仍需用 singleflight 保证同一个 asOf 只生成一次，避免并发首访重复付出
// 一次 O(N)+O(M·log M) 的阻塞成本。
func (f *FeedService) buildHotSnapshot(asOf time.Time) error {
	dest := f.hotSnapshotKey(asOf)
	keys := f.hotWindowKeys(asOf)

	// 一律基于 Background 而不是请求 ctx：快照是多个请求共享的结果，leader 的请求
	// 取消不应让所有等待者一起失败。预算由每一步自己的 timeout 控制。
	unionCtx, unionCancel := context.WithTimeout(context.Background(), hotMergeTimeout)
	defer unionCancel()
	if err := f.rediscache.ZUnionStore(unionCtx, dest, keys, "SUM"); err != nil {
		return err
	}

	// 只保留 Top-N：把 O(M·log M) 里的 M 和快照内存都压到常数。
	trimCtx, trimCancel := context.WithTimeout(context.Background(), singleRedisOpTimeout)
	defer trimCancel()
	if err := f.rediscache.ZRemRangeByRank(trimCtx, dest, 0, -(hotSnapshotCap + 1)); err != nil {
		return err
	}

	cardCtx, cardCancel := context.WithTimeout(context.Background(), singleRedisOpTimeout)
	defer cardCancel()
	card, err := f.rediscache.ZCard(cardCtx, dest)
	if err != nil {
		return err
	}

	// 空窗口是合法结果（最近一小时确实没有热度）。记一个标记，避免 EXISTS
	// 永远 miss、每个请求都重跑一次合并。不要把它当成「失败」去降级：
	// MySQL 那条路是累计热度榜，排序语义完全不同。
	if card == 0 {
		emptyCtx, emptyCancel := context.WithTimeout(context.Background(), singleRedisOpTimeout)
		defer emptyCancel()
		return f.rediscache.SetBytes(emptyCtx, f.hotSnapshotEmptyKey(asOf), []byte("1"), hotSnapshotTTL)
	}

	expireCtx, expireCancel := context.WithTimeout(context.Background(), singleRedisOpTimeout)
	defer expireCancel()
	return f.rediscache.Expire(expireCtx, dest, hotSnapshotTTL)
}

// ensureHotSnapshot 保证 asOf 对应的快照存在，并返回它是否可用。
//
// 返回 false 只代表「Redis 读失败」或「合并失败」，调用方应降级到 MySQL；
// 空窗口返回 true（空标记存在），因为「最近一小时没有热度」也是一个有效答案。
//
// singleflight 按 asOf 去重，一次解决两个问题：
//  1. 性能：避免快照过期的瞬间 N 个并发请求各自跑一次 60 桶 ZUNIONSTORE。
//     ZUNIONSTORE 是阻塞命令、跑在 Redis 单线程上，N 份重复计算会把整个实例钉住。
//  2. 正确性：保证所有请求读到的是同一份快照，这是「翻页期间排序键不变」的前提。
func (f *FeedService) ensureHotSnapshot(asOf time.Time) bool {
	dest := f.hotSnapshotKey(asOf)
	emptyKey := f.hotSnapshotEmptyKey(asOf)

	if exists, err := f.hotKeyExists(dest); err != nil {
		return false
	} else if exists {
		return true
	}
	if exists, err := f.hotKeyExists(emptyKey); err != nil {
		return false
	} else if exists {
		return true
	}

	sfKey := f.rediscache.Key("sf:hot:merge:%s", asOf.Format("200601021504"))
	_, err, _ := f.requestGroup.Do(sfKey, func() (interface{}, error) {
		// 双重检查：等待 singleflight 期间可能已被别的 goroutine 生成。
		if exists, err := f.hotKeyExists(dest); err != nil {
			return nil, err
		} else if exists {
			return nil, nil
		}
		if exists, err := f.hotKeyExists(emptyKey); err != nil {
			return nil, err
		} else if exists {
			return nil, nil
		}
		return nil, f.buildHotSnapshot(asOf)
	})
	if err != nil {
		// 必须留痕。静默吞掉的话快照永远不存在，而 ZREVRANGE 对不存在的 key 返回
		// 空且不报错 —— 热榜会「看起来正常」地整体降级到 MySQL，没人能发现。
		log.Printf("hot: build snapshot as_of=%d failed, degrading to MySQL: %v", asOf.Unix(), err)
		return false
	}
	return true
}

// hotKeyExists 是带独立超时预算的 EXISTS。调用方需要区分「不存在」和「Redis 出错」：
// 后者不能当成空窗口处理，否则一次连接抖动就会被标记成「最近一小时没热度」。
func (f *FeedService) hotKeyExists(key string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), singleRedisOpTimeout)
	defer cancel()
	return f.rediscache.Exists(ctx, key)
}

// StartHotSnapshotBuilder 后台预生成「最后封桶分钟」的榜单快照。
//
// 为什么必须搬出请求路径：ZUNIONSTORE 合并 60 个分钟桶是 O(N) + O(M·log M) 的阻塞
// 命令，跑在 Redis 单线程上。百万级视频下单次合并要数秒 —— 留在读路径上意味着每个
// 快照过期的瞬间都至少有一个请求被钉住，并阻塞同实例上的所有其它命令。
// 预生成之后，读路径只剩一次 EXISTS + 一次 O(log N + M) 的 ZREVRANGE。
//
// 读路径仍保留 on-demand 合并作为兜底（预生成没跟上、或客户端传了历史 as_of），
// 所以这个函数不工作也不会让热榜不可用，只会退化回原来的行为。
func StartHotSnapshotBuilder(ctx context.Context, svc *FeedService, interval time.Duration) {
	if svc == nil || svc.rediscache == nil {
		log.Printf("hot snapshot builder disabled: feed service or redis unavailable")
		return
	}
	if interval <= 0 {
		interval = time.Minute
	}
	buildLatest := func() {
		svc.ensureHotSnapshot(hotLatestSealedMinute(time.Now()))
	}
	go func() {
		buildLatest()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				buildLatest()
			}
		}
	}()
}

// StartTimelineReconciler 定期用 MySQL 的最新 N 条校验热时间线，补齐缺失成员。
//
// 空 ZSET 有读路径的 singleflight 重建兜底；这条任务解决的是「非空但少了几条」：
// 旧实现只判断 ZSET 是否为空，部分丢失会一直存在，表现为某一页少内容。
func StartTimelineReconciler(ctx context.Context, svc *FeedService, interval time.Duration) {
	if svc == nil || svc.rediscache == nil {
		log.Printf("timeline reconciler disabled: feed service or redis unavailable")
		return
	}
	if interval <= 0 {
		interval = TimelineReconcileInterval
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				svc.reconcileTimeline(ctx)
			}
		}
	}()
}

func (f *FeedService) reconcileTimeline(ctx context.Context) {
	key := video.TimelineZSetKey(f.rediscache)

	var dbVideos []*video.Video
	if err := dbguard.Do(func() error {
		var e error
		dbVideos, e = f.repo.ListLatest(ctx, video.TimelineCapacity, nil)
		return e
	}); err != nil {
		log.Printf("timeline reconcile: 查询 MySQL 失败: %v", err)
		return
	}
	if len(dbVideos) == 0 {
		return
	}

	readCtx, readCancel := context.WithTimeout(ctx, singleRedisOpTimeout)
	existing, err := f.rediscache.ZRangeWithScores(readCtx, key, 0, -1)
	readCancel()
	if err != nil {
		log.Printf("timeline reconcile: 读取 ZSET 失败: %v", err)
		return
	}

	have := make(map[string]struct{}, len(existing))
	for _, z := range existing {
		if member, ok := z.Member.(string); ok {
			have[member] = struct{}{}
		}
	}

	missing := make([]redis.Z, 0, len(dbVideos))
	for _, v := range dbVideos {
		member := strconv.FormatUint(uint64(v.ID), 10)
		if _, ok := have[member]; ok {
			continue
		}
		missing = append(missing, redis.Z{
			Score:  float64(v.CreateTime.UnixMilli()),
			Member: member,
		})
	}
	if len(missing) == 0 {
		return
	}

	writeCtx, writeCancel := context.WithTimeout(ctx, 2*time.Second)
	defer writeCancel()
	if err := f.rediscache.ZAdd(writeCtx, key, missing...); err != nil {
		log.Printf("timeline reconcile: 补齐失败(%d 条): %v", len(missing), err)
		return
	}
	// 只补不删，但容量上限仍要维持
	_ = f.rediscache.ZRemRangeByRank(writeCtx, key, 0, -(video.TimelineCapacity + 1))
	log.Printf("timeline reconcile: 补齐缺失热数据 %d 条", len(missing))
}

func (f *FeedService) ListByPopularity(ctx context.Context, limit int, reqAsOf int64, offset int, viewerAccountID uint, latestPopularity int64, latestBefore time.Time, latestIDBefore uint, forceDB bool) (ListByPopularityResponse, error) {
	// offset 由 handler 保证非负；这里再兜一层，避免负数被 Redis 当成「从尾部取」。
	if offset < 0 {
		offset = 0
	}

	// Redis 热榜（稳定分页：as_of + offset）。
	//
	// forceDB 用于「已经降级的分页会话」：Redis 中途恢复时不能悄悄切回窗口榜，
	// 否则同一个会话会把累计热度榜和窗口热度榜拼在一起，出现重复/跳变。
	if f.rediscache != nil && !forceDB {
		latest := hotLatestSealedMinute(time.Now())
		earliest := latest.Add(-hotAsOfLookback)

		asOf := latest
		if reqAsOf > 0 {
			asOf = time.Unix(reqAsOf, 0).UTC().Truncate(time.Minute)
			if asOf.After(latest) || asOf.Before(earliest) {
				// as_of 不在可翻页区间内（太老、或旧客户端传了当前/未来分钟）。
				// 第一页可以直接换新榜；「加载更多」必须明确终止并让客户端刷新，
				// 绝不能把两套窗口的数据拼进同一个会话。
				if offset > 0 {
					return ListByPopularityResponse{
						VideoList: []FeedVideoItem{},
						Expired:   true,
					}, nil
				}
				asOf = latest
			}
		}

		// 快照存在时这里只花一次 EXISTS；缺失时才合并，且合并被 singleflight
		// 按 as_of 收敛成一次（见 ensureHotSnapshot）。
		if f.ensureHotSnapshot(asOf) {
			dest := f.hotSnapshotKey(asOf)

			// 分页读取单独一份预算，不再和最贵的 ZUNIONSTORE 共享同一个 context。
			readCtx, cancel := context.WithTimeout(ctx, singleRedisOpTimeout)
			members, err := f.rediscache.ZRevRange(readCtx, dest, int64(offset), int64(offset+limit-1))
			cancel()

			if err == nil {
				// 快照可用就按快照返回，哪怕为空：窗口内确实没有热度时，
				// 返回空榜才是正确语义，不能悄悄换成 MySQL 的累计热度榜。
				ids := make([]uint, 0, len(members))
				for _, m := range members {
					u, err := strconv.ParseUint(m, 10, 64)
					if err == nil && u > 0 {
						ids = append(ids, uint(u))
					}
				}

				// 热榜的「排序」与「内容」是分离的：ZSET 快照只给出 ID 顺序，
				// 内容按 ID 走实体三级缓存装配 —— 与最新流、详情页共用同一份
				// video:entity:<id>，热门视频在三条路径上只需缓存一份。
				// GetVideoByIDs 已按入参顺序返回并跳过缺失项，等价于原先的手工重排。
				//
				// hydrate 失败必须直接返回：回退到 MySQL 只会让已经过载的库
				// 再挨一轮，而且会让 offset 推进与返回内容错位。
				videos, err := f.GetVideoByIDs(ctx, ids)
				if err != nil {
					return ListByPopularityResponse{}, err
				}
				items, err := f.buildFeedVideos(ctx, videos, viewerAccountID)
				if err != nil {
					return ListByPopularityResponse{}, err
				}
				// 推进量必须是「从快照里取走了几个 member」，而不是 hydrate 之后
				// 剩几条：视频被删除时 len(items) < len(members)，按 items 推进
				// 会少前进，下一页就把本页最后一条再返回一次。
				resp := ListByPopularityResponse{
					VideoList:  items,
					AsOf:       asOf.Unix(),
					NextOffset: offset + len(members),
					HasMore:    len(members) == limit,
				}
				if len(videos) > 0 {
					last := videos[len(videos)-1]
					nextPopularity := last.Popularity
					nextBefore := last.CreateTime
					nextID := last.ID
					resp.NextLatestPopularity = &nextPopularity
					resp.NextLatestBefore = &nextBefore
					resp.NextLatestIDBefore = &nextID
				}
				return resp, nil
			}
		}
	}

	// 降级态（Redis 不可用 / 快照合并或读取失败 / 客户端要求继续 DB 会话）只剩
	// MySQL 这一条路，而 MySQL 侧只认 (popularity, create_time, id) 复合游标，
	// 不认 offset。客户端没把游标带回来时，绝不能带着空游标去查 —— 那等价于从头
	// 查，前端「加载更多」会把第一页再拼一遍。宁可明确终止，也不返回错位的数据。
	if offset > 0 && (latestBefore.IsZero() || latestIDBefore == 0) {
		return ListByPopularityResponse{
			VideoList: []FeedVideoItem{},
			Degraded:  true,
		}, nil
	}

	var videos []*video.Video
	if err := dbguard.Do(func() error {
		var e error
		videos, e = f.repo.ListByPopularity(ctx, limit, latestPopularity, latestBefore, latestIDBefore)
		return e
	}); err != nil {
		return ListByPopularityResponse{}, err
	}
	items, err := f.buildFeedVideos(ctx, videos, viewerAccountID)
	if err != nil {
		return ListByPopularityResponse{}, err
	}
	resp := ListByPopularityResponse{
		VideoList:  items,
		AsOf:       0,
		NextOffset: 0,
		HasMore:    len(items) == limit,
		// 明确告诉客户端：这是累计热度排序，不是窗口热榜。
		Degraded: true,
	}
	if len(videos) > 0 {
		last := videos[len(videos)-1]
		nextPopularity := last.Popularity
		nextBefore := last.CreateTime
		nextID := last.ID
		resp.NextLatestPopularity = &nextPopularity
		resp.NextLatestBefore = &nextBefore
		resp.NextLatestIDBefore = &nextID
	}
	return resp, nil
}

func (f *FeedService) buildFeedVideos(ctx context.Context, videos []*video.Video, viewerAccountID uint) ([]FeedVideoItem, error) {
	feedVideos := make([]FeedVideoItem, 0, len(videos))
	videoIDs := make([]uint, len(videos))
	for i, v := range videos {
		videoIDs[i] = v.ID
	}
	// is_liked 是 viewer 维度的高频字段，每页都要查一次库。它同样是回源入口：
	// 库已经过载时不该再为辅助字段加压，查不到就降级成「都没点过」——
	// 点赞写入本身有唯一索引兜底，重复点不会写坏数据。
	var likedMap map[uint]bool
	if viewerAccountID == 0 {
		likedMap = map[uint]bool{}
	} else if err := dbguard.Do(func() error {
		var e error
		likedMap, e = f.likeRepo.BatchGetLiked(ctx, videoIDs, viewerAccountID)
		return e
	}); err != nil {
		log.Printf("feed: liked map unavailable, degrade is_liked=false: %v", err)
		likedMap = map[uint]bool{}
	}
	for _, video := range videos {
		feedVideos = append(feedVideos, FeedVideoItem{
			ID:          video.ID,
			Author:      FeedAuthor{ID: video.AuthorID, Username: video.Username},
			Title:       video.Title,
			Description: video.Description,
			PlayURL:     f.resolveURL(ctx, video.PlayURL),
			CoverURL:    f.resolveURL(ctx, video.CoverURL),
			CreateTime:  video.CreateTime.Unix(),
			LikesCount:  video.LikesCount,
			IsLiked:     likedMap[video.ID],
		})
	}
	return feedVideos, nil
}

// resolveURL 把 object key 解析成可访问 URL（预签名 / 本地静态路径）。
// 数据库里只存 key，这里在组装响应时现签，避免过期 URL 落库。
func (f *FeedService) resolveURL(ctx context.Context, key string) string {
	if key == "" || f.storage == nil {
		return key
	}
	u, err := f.storage.Resolve(ctx, key)
	if err != nil {
		return key
	}
	return u
}

func buildOrderedResult(orderedIDs []uint, dataMap map[uint]*video.Video) []*video.Video {
	res := make([]*video.Video, 0, len(orderedIDs))
	for _, id := range orderedIDs {
		if v, exits := dataMap[id]; exits && v != nil {
			res = append(res, v)
		}
	}
	return res
}

func (f *FeedService) ListTags(ctx context.Context, limit int) ([]TagSummary, error) {
	var tags []TagSummary
	if err := dbguard.Do(func() error {
		var e error
		tags, e = f.repo.ListTags(ctx, limit)
		return e
	}); err != nil {
		return nil, err
	}
	return tags, nil
}

// ListByTag 与 ListLatest 使用同一套复合游标：多取一条判断 has_more，
// 避免「本页有视频被删除/过滤」时提前判定到底。
func (f *FeedService) ListByTag(ctx context.Context, tagName string, limit int, cursor *TimelineCursor, viewerAccountID uint) (ListByTagResponse, error) {
	var videos []*video.Video
	if err := dbguard.Do(func() error {
		var e error
		videos, e = f.repo.ListByTag(ctx, tagName, limit+1, cursor)
		return e
	}); err != nil {
		return ListByTagResponse{}, err
	}
	hasMore := len(videos) > limit
	if hasMore {
		videos = videos[:limit]
	}
	feedVideos, err := f.buildFeedVideos(ctx, videos, viewerAccountID)
	if err != nil {
		return ListByTagResponse{}, err
	}
	resp := ListByTagResponse{
		VideoList: feedVideos,
		HasMore:   hasMore,
	}
	if len(videos) > 0 {
		last := videos[len(videos)-1]
		nextID := last.ID
		resp.NextTime = last.CreateTime.UnixMilli()
		resp.NextIDBefore = &nextID
	}
	return resp, nil
}

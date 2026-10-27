package feed

import (
	"context"
	"encoding/json"
	"feedsystem_video_go/internal/apierror"
	"feedsystem_video_go/internal/config"
	rediscache "feedsystem_video_go/internal/middleware/redis"
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

// hotSnapshotTTL 是榜单快照的存活时间。
// 下界必须 > 1 分钟：只有等 asOf 所在分钟桶封桶之后重建快照，重建结果才与首份
// 逐位一致。2 分钟 = 1 分钟封桶 + 1 分钟余量，不是随手取的值。
const hotSnapshotTTL = 2 * time.Minute

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

// maxConcurrentDBLoad 是单个进程内允许同时打向 MySQL 的回源查询数上限。
//
// 为什么需要它：Redis 整体不可用时，缓存层不再拦截任何流量 ——
// GetVideoByIDs 会走「MGet 报错 → 全量降级到 L3」分支，ListByFollowing
// 在 Redis 连接错误时也会直接兜底查库。此时 singleflight 只能把 N 个请求
// 收敛到 P（Pod 数）个，若不对回源并发设上限，压力会毫无衰减地压到数据库
// 上，这就是缓存雪崩。
//
// 实现：带缓冲 channel 当信号量，**非阻塞**获取。拿不到配额就放弃本次回源
// 并降级（实体路径表现为该条不出现在结果里，关注流返回 errDBOverloaded），
// 而不是排队等待 —— 排队只是把堆积从数据库搬到应用内存，压力最终还是会落到库上。
//
// 取值取 config.DefaultMaxConcurrentDBLoad（连接池上限的一半）：这是应用层
// 的「准入闸」，连接池是驱动层的「硬闸门」，两道都要有。若通过
// MYSQL_MAX_OPEN_CONNS 调大了连接池，这里也要同步调大。
var maxConcurrentDBLoad = config.DefaultMaxConcurrentDBLoad

// errDBOverloaded 表示回源并发已达上限，本次请求被主动丢弃以保护数据库。
// 它包装 apierror.ErrServiceOverloaded，HTTP 层会把它分类成 503 而不是 500 ——
// 这是「主动限流降级」而不是「服务故障」，客户端收到 503 才知道该退避重试。
var errDBOverloaded = fmt.Errorf("feed: db load gate full, shedding load: %w", apierror.ErrServiceOverloaded)

type FeedService struct {
	repo         *FeedRepository
	likeRepo     *video.LikeRepository
	rediscache   *rediscache.Client
	localcache   *cache.Cache
	cacheTTL     time.Duration
	requestGroup singleflight.Group
	dbLoadGate   chan struct{} // 回源并发闸门，见 maxConcurrentDBLoad
}

type CachedFeedData struct {
	PublicVideos []video.Video `json:"public_videos"`
}

func NewFeedService(repo *FeedRepository, likeRepo *video.LikeRepository, rediscache *rediscache.Client) *FeedService {
	return &FeedService{
		repo:       repo,
		likeRepo:   likeRepo,
		rediscache: rediscache,
		localcache: cache.New(3*time.Second, 5*time.Second),
		cacheTTL:   24 * time.Hour,
		dbLoadGate: make(chan struct{}, maxConcurrentDBLoad),
	}
}

// acquireDBGate 尝试占用一个回源配额。非阻塞：拿不到立即返回 false，
// 调用方应当放弃本次回源并降级，而不是排队把压力继续堆到 MySQL 上。
func (f *FeedService) acquireDBGate() bool {
	if f.dbLoadGate == nil { // 未经构造函数直接构造时不限流，避免误伤
		return true
	}
	select {
	case f.dbLoadGate <- struct{}{}:
		return true
	default:
		return false
	}
}

func (f *FeedService) releaseDBGate() {
	if f.dbLoadGate == nil {
		return
	}
	<-f.dbLoadGate
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
			log.Printf("L2 Redis MGet 失败，全部降级到 MySQL: %v", err)
		}
	}

	if len(missedL2) == 0 {
		return buildOrderedResult(videoIDs, videoMap), nil
	}

	//L3:MySQL
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, id := range missedL2 {
		wg.Add(1)
		go func(videoID uint) {
			defer wg.Done()
			// 与详情页 GetDetail 共用同一个回源去重组：两条路径命中的是同一个
			// video:entity:<id>，只有共用 Group 与 key，并发回源才能真正合并。
			v, err, _ := video.EntityLoadGroup.Do(video.EntitySingleflightKey(videoID), func() (interface{}, error) {
				// Redis 宕机全量降级时，这个闸门是唯一挡在 MySQL 前面的东西：
				// 拿不到配额就放弃本次回源（该条不出现在结果里），而不是把库打爆。
				if !f.acquireDBGate() {
					return nil, nil
				}
				defer f.releaseDBGate()

				videoList, err := f.repo.GetByIDs(ctx, []uint{videoID})
				if err != nil {
					return nil, err
				}
				if len(videoList) == 0 {
					// 记下「这个 ID 确实不存在」，否则同一个不存在的 ID 每次请求都会
					// 打一次 MySQL —— 这就是缓存穿透。
					// 刷新缓存不受请求生命周期限制，客户端断开也要能写进去。
					go func(id uint) {
						video.SetVideoEntityNull(context.Background(), f.rediscache, id)
					}(videoID)
					return nil, nil
				}

				safeCopy := *videoList[0]
				cachekey := video.VideoEntityKey(f.rediscache, safeCopy.ID)
				if b, err := json.Marshal(safeCopy); err == nil {
					//异步回写redis
					go func(k string, b []byte) {
						setCtx, setCancel := context.WithTimeout(context.Background(), singleRedisOpTimeout)
						defer setCancel()

						f.rediscache.SetBytes(setCtx, k, b, video.VideoEntityTTL)
					}(cachekey, b)
				}
				return videoList[0], err
			})

			if err == nil && v != nil {
				safeCopy := *(v.(*video.Video))
				mu.Lock()
				videoMap[id] = &safeCopy
				mu.Unlock()
				f.localcache.Set(video.VideoEntityKey(f.rediscache, safeCopy.ID), safeCopy, 5*time.Second)
			}
		}(id)
	}
	wg.Wait()
	return buildOrderedResult(videoIDs, videoMap), nil
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

	timelineKey := f.rediscache.Key("feed:global_timeline")

	// 获取 ZSET 中最老的一条数据
	zsetTail, err := f.rediscache.ZRangeWithScores(ctx, timelineKey, 0, 0)

	if err != nil {
		return f.listLatestFromDB(ctx, limit, cursor, viewerAccountID)
	}

	if len(zsetTail) == 0 {
		//全局静态锁：无视所有用户的不同游标
		sfKey := f.rediscache.Key("sf:fallback:global_timeline_rebuild")

		v, err, _ := f.requestGroup.Do(sfKey, func() (interface{}, error) {
			// 无视游标，直接去 MySQL 捞最新的 1000 条
			dbVideos, err := f.repo.ListLatest(ctx, 1000, nil)
			if err != nil {
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
			f.rediscache.ZAdd(bgCtx, timelineKey, zElements...)
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

	var baseVideos []*video.Video

	if cursor != nil && reqTime <= watermark {
		// ===== 冷区：整页走 MySQL =====

		// 针对个别用户的防并发（此时可以用游标做锁，因为冷尾流量极小）
		sfKey := f.rediscache.Key("sf:cold:listLatest:%d:%d:%d", limit, reqTime, cursor.ID)
		v, err, _ := f.requestGroup.Do(sfKey, func() (interface{}, error) {
			return f.repo.ListLatest(ctx, limit, cursor)
		})
		if err != nil {
			return ListLatestResponse{}, err
		}
		baseVideos = v.([]*video.Video)
		// 不回写 ZSET，防止冷数据污染热点时间线

	} else {
		// ===== 热区：整页走 Redis ZSET =====
		videoIDs, err := f.listHotIDs(ctx, timelineKey, limit, cursor, reqTime)
		if err != nil {
			return ListLatestResponse{}, err
		}

		if len(videoIDs) > 0 {
			baseVideos, err = f.GetVideoByIDs(ctx, videoIDs)
			if err != nil {
				return ListLatestResponse{}, err
			}
		}

		// 刚好击穿了冷热边界：用热区最后一条的**复合**游标去 MySQL 补齐。
		// MySQL 是 ZSET 的超集，所以从这里继续往下取不会漏；
		// 复合比较又会排除热区已经给出的那一条及其同毫秒的兄弟，不会重复。
		if len(baseVideos) < limit {
			remainLimit := limit - len(baseVideos) // 计算还差几个

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
				return f.repo.ListLatest(ctx, remainLimit, coldCursor)
			})

			if err == nil {
				coldVideos := v.([]*video.Video)
				baseVideos = append(baseVideos, coldVideos...)
			}
		}
	}

	resp := ListLatestResponse{HasMore: len(baseVideos) == limit}
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
func (f *FeedService) listHotIDs(ctx context.Context, key string, limit int, cursor *TimelineCursor, reqTime int64) ([]uint, error) {
	maxScore := "+inf"
	count := int64(limit)

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

	if len(entries) > limit {
		entries = entries[:limit]
	}

	ids := make([]uint, 0, len(entries))
	for _, e := range entries {
		ids = append(ids, e.id)
	}
	return ids, nil
}

func (f *FeedService) listLatestFromDB(ctx context.Context, limit int, cursor *TimelineCursor, viewerAccountID uint) (ListLatestResponse, error) {
	videos, err := f.repo.ListLatest(ctx, limit, cursor)
	if err != nil {
		return ListLatestResponse{}, err
	}
	feedVideos, err := f.buildFeedVideos(ctx, videos, viewerAccountID)
	if err != nil {
		return ListLatestResponse{}, err
	}
	resp := ListLatestResponse{
		VideoList: feedVideos,
		HasMore:   len(videos) == limit,
	}
	if len(videos) > 0 {
		last := videos[len(videos)-1]
		nextID := last.ID
		resp.NextTime = last.CreateTime.UnixMilli()
		resp.NextIDBefore = &nextID
	}
	return resp, nil
}

// 按照点赞数查询视频
func (f *FeedService) ListLikesCount(ctx context.Context, limit int, cursor *LikesCountCursor, viewerAccountID uint) (ListLikesCountResponse, error) {
	videos, err := f.repo.ListLikesCountWithCursor(ctx, limit, cursor)
	if err != nil {
		return ListLikesCountResponse{}, err
	}
	hasMore := len(videos) == limit
	feedVideos, err := f.buildFeedVideos(ctx, videos, viewerAccountID)
	if err != nil {
		return ListLikesCountResponse{}, err
	}
	resp := ListLikesCountResponse{
		VideoList: feedVideos,
		HasMore:   hasMore,
	}
	if len(videos) > 0 {
		last := videos[len(videos)-1]
		nextLikesCountBefore := last.LikesCount
		nextIDBefore := last.ID
		resp.NextLikesCountBefore = &nextLikesCountBefore
		resp.NextIDBefore = &nextIDBefore
	}
	return resp, nil
}

// 按照关注列表查询视频
// ListByFollowing 与 ListLatest 共用同一套复合游标语义。
// 游标单位统一为**毫秒**（历史上这里用的是秒，与 listLatest 不一致，属于契约缺陷）。
func (f *FeedService) ListByFollowing(ctx context.Context, limit int, cursor *TimelineCursor, viewerAccountID uint) (ListByFollowingResponse, error) {
	doListByFollowingFromDB := func() (ListByFollowingResponse, error) {
		// 这是 JOIN 关注表 + 排序的昂贵查询，而且 Redis 宕机时 SETNX 锁失效、
		// 所有请求都会直接落到这里 —— 是最需要先挡住的回源路径。
		if !f.acquireDBGate() {
			return ListByFollowingResponse{}, errDBOverloaded
		}
		defer f.releaseDBGate()

		videos, err := f.repo.ListByFollowing(ctx, limit, viewerAccountID, cursor)
		if err != nil {
			return ListByFollowingResponse{}, err
		}
		feedVideos, err := f.buildFeedVideos(ctx, videos, viewerAccountID)
		if err != nil {
			return ListByFollowingResponse{}, err
		}
		resp := ListByFollowingResponse{
			VideoList: feedVideos,
			HasMore:   len(videos) == limit,
		}
		if len(videos) > 0 {
			last := videos[len(videos)-1]
			nextID := last.ID
			resp.NextTime = last.CreateTime.UnixMilli()
			resp.NextIDBefore = &nextID
		}
		return resp, nil
	}
	var cacheKey string
	if viewerAccountID != 0 && f.rediscache != nil {
		before := int64(0)
		idBefore := uint(0)
		if cursor != nil {
			before = cursor.CreateTime.UnixMilli()
			idBefore = cursor.ID
		}
		// 游标进入了 key：关注流的整页缓存必须按完整游标区分，否则不同页会串味
		cacheKey = f.rediscache.Key("feed:listByFollowing:limit=%d:accountID=%d:before=%d:id=%d", limit, viewerAccountID, before, idBefore)

		// 每个 Redis 操作都要各自申请一份超时预算：同一个 context 的额度会被
		// 串联在上面的操作连续消耗，共用的话最后一步回填必然超时。
		getCached := func() (ListByFollowingResponse, bool) {
			opCtx, cancel := context.WithTimeout(ctx, singleRedisOpTimeout)
			defer cancel()

			b, err := f.rediscache.GetBytes(opCtx, cacheKey)
			if err != nil {
				return ListByFollowingResponse{}, false
			}
			var cached ListByFollowingResponse
			if err := json.Unmarshal(b, &cached); err != nil {
				return ListByFollowingResponse{}, false
			}
			return cached, true
		}
		setCached := func(resp ListByFollowingResponse) {
			b, err := json.Marshal(resp)
			if err != nil {
				return
			}
			opCtx, cancel := context.WithTimeout(ctx, singleRedisOpTimeout)
			defer cancel()
			_ = f.rediscache.SetBytes(opCtx, cacheKey, b, f.cacheTTL)
		}

		readCtx, readCancel := context.WithTimeout(ctx, singleRedisOpTimeout)
		b, err := f.rediscache.GetBytes(readCtx, cacheKey)
		readCancel()

		if err == nil {
			var cached ListByFollowingResponse
			if err := json.Unmarshal(b, &cached); err == nil {
				return cached, nil
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
					return cached, nil
				}

				resp, err := doListByFollowingFromDB()
				if err != nil {
					return ListByFollowingResponse{}, err
				}
				setCached(resp)
				return resp, nil
			}

			// 没抢到锁：轮询等待持锁者回填，同时响应客户端取消
			for i := 0; i < 5; i++ {
				select {
				case <-ctx.Done():
					return ListByFollowingResponse{}, ctx.Err()
				case <-time.After(20 * time.Millisecond):
				}
				if cached, ok := getCached(); ok {
					return cached, nil
				}
			}
		}
	}

	resp, err := doListByFollowingFromDB()
	if err != nil {
		return ListByFollowingResponse{}, err
	}
	if cacheKey != "" {
		if b, err := json.Marshal(resp); err == nil {
			writeCtx, writeCancel := context.WithTimeout(ctx, singleRedisOpTimeout)
			defer writeCancel()
			_ = f.rediscache.SetBytes(writeCtx, cacheKey, b, f.cacheTTL)
		}
	}
	return resp, nil
}

// hotSnapshotKey 返回 asOf 时刻对应的榜单快照 key。
// key 里带分钟级 as_of，因此不同分钟的快照互不干扰，也不会出现大批 key 同时失效。
func (f *FeedService) hotSnapshotKey(asOf time.Time) string {
	return f.rediscache.Key("hot:video:merge:1m:%s", asOf.Format("200601021504"))
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
// 调用方必须保证同一个 asOf 只有一个 goroutine 在执行（由 ensureHotSnapshot 的
// singleflight 保证）。并发执行是错的：asOf 所在分钟桶此时可能尚未封桶，两次合并
// 之间新写入的热度会让两份快照不一致，后完成的覆盖先完成的 —— 先到的用户翻下一页
// 时读到的就是另一份榜单，排序键在翻页期间发生了变化。
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

	expireCtx, expireCancel := context.WithTimeout(context.Background(), singleRedisOpTimeout)
	defer expireCancel()
	return f.rediscache.Expire(expireCtx, dest, hotSnapshotTTL)
}

// ensureHotSnapshot 保证 asOf 对应的快照存在。
//
// singleflight 按 asOf 去重，一次解决两个问题：
//  1. 性能：避免快照过期的瞬间 N 个并发请求各自跑一次 60 桶 ZUNIONSTORE。
//     ZUNIONSTORE 是阻塞命令、跑在 Redis 单线程上，N 份重复计算会把整个实例钉住。
//  2. 正确性：保证所有请求读到的是同一份快照，这是「翻页期间排序键不变」的前提。
func (f *FeedService) ensureHotSnapshot(asOf time.Time) {
	dest := f.hotSnapshotKey(asOf)

	existsCtx, existsCancel := context.WithTimeout(context.Background(), singleRedisOpTimeout)
	defer existsCancel()
	if exists, _ := f.rediscache.Exists(existsCtx, dest); exists {
		return
	}

	sfKey := f.rediscache.Key("sf:hot:merge:%s", asOf.Format("200601021504"))
	_, err, _ := f.requestGroup.Do(sfKey, func() (interface{}, error) {
		// 双重检查：等待 singleflight 期间可能已被别的 goroutine 生成。
		checkCtx, checkCancel := context.WithTimeout(context.Background(), singleRedisOpTimeout)
		defer checkCancel()
		if exists, _ := f.rediscache.Exists(checkCtx, dest); exists {
			return nil, nil
		}
		return nil, f.buildHotSnapshot(asOf)
	})
	if err != nil {
		// 必须留痕。静默吞掉的话快照永远不存在，而 ZREVRANGE 对不存在的 key 返回
		// 空且不报错 —— 热榜会「看起来正常」地整体降级到 MySQL，没人能发现。
		log.Printf("hot: build snapshot as_of=%d failed, degrading to MySQL: %v", asOf.Unix(), err)
	}
}

// StartHotSnapshotBuilder 后台预生成「当前分钟」的榜单快照。
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
	go func() {
		svc.ensureHotSnapshot(time.Now().UTC().Truncate(time.Minute))
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				svc.ensureHotSnapshot(time.Now().UTC().Truncate(time.Minute))
			}
		}
	}()
}

func (f *FeedService) ListByPopularity(ctx context.Context, limit int, reqAsOf int64, offset int, viewerAccountID uint, latestPopularity int64, latestBefore time.Time, latestIDBefore uint) (ListByPopularityResponse, error) {
	// Redis 热榜（稳定分页：as_of + offset）
	if f.rediscache != nil {
		asOf := time.Now().UTC().Truncate(time.Minute)
		if reqAsOf > 0 {
			asOf = time.Unix(reqAsOf, 0).UTC().Truncate(time.Minute)
		}

		// 快照存在时这里只花一次 EXISTS；缺失时才合并，且合并被 singleflight
		// 按 as_of 收敛成一次（见 ensureHotSnapshot）。
		f.ensureHotSnapshot(asOf)
		dest := f.hotSnapshotKey(asOf)

		// 分页读取单独一份预算，不再和最贵的 ZUNIONSTORE 共享同一个 context。
		readCtx, cancel := context.WithTimeout(ctx, singleRedisOpTimeout)
		defer cancel()

		start := int64(offset)
		stop := start + int64(limit) - 1
		members, err := f.rediscache.ZRevRange(readCtx, dest, start, stop)
		if err == nil && len(members) == 0 {
			if offset > 0 {
				return ListByPopularityResponse{
					VideoList:  []FeedVideoItem{},
					AsOf:       asOf.Unix(),
					NextOffset: offset,
					HasMore:    false,
				}, nil
			}
		}
		if err == nil && len(members) > 0 {
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
			videos, err := f.GetVideoByIDs(ctx, ids)
			if err == nil {
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

	// 降级态（Redis 不可用 / 快照读取失败）只剩 MySQL 这一条路，而 MySQL 侧只认
	// (popularity, create_time, id) 复合游标，不认 offset。客户端没把游标带回来时，
	// 绝不能带着空游标去查 —— 那等价于从头查，前端「加载更多」会把第一页再拼一遍。
	// 宁可明确终止（返回空 + has_more=false），也不返回错位的数据。
	if offset > 0 && (latestBefore.IsZero() || latestIDBefore == 0) {
		return ListByPopularityResponse{
			VideoList: []FeedVideoItem{},
			AsOf:      0,
		}, nil
	}

	videos, err := f.repo.ListByPopularity(ctx, limit, latestPopularity, latestBefore, latestIDBefore)
	if err != nil {
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
	likedMap, err := f.likeRepo.BatchGetLiked(ctx, videoIDs, viewerAccountID)
	if err != nil {
		return nil, err
	}
	for _, video := range videos {
		feedVideos = append(feedVideos, FeedVideoItem{
			ID:          video.ID,
			Author:      FeedAuthor{ID: video.AuthorID, Username: video.Username},
			Title:       video.Title,
			Description: video.Description,
			PlayURL:     video.PlayURL,
			CoverURL:    video.CoverURL,
			CreateTime:  video.CreateTime.Unix(),
			LikesCount:  video.LikesCount,
			IsLiked:     likedMap[video.ID],
		})
	}
	return feedVideos, nil
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

func (f *FeedService) ListByTag(ctx context.Context, tagName string, limit int, viewerAccountID uint) ([]FeedVideoItem, error) {
	videos, err := f.repo.ListByTag(ctx, tagName, limit)
	if err != nil {
		return nil, err
	}
	return f.buildFeedVideos(ctx, videos, viewerAccountID)
}

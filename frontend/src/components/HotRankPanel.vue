<script setup lang="ts">
import { computed, onMounted, reactive } from 'vue'

import { ApiError } from '../api/client'
import * as feedApi from '../api/feed'
import * as likeApi from '../api/like'
import type { FeedVideoItem } from '../api/types'
import { useAuthStore } from '../stores/auth'
import { useToastStore } from '../stores/toast'
import FeedVideoCard from './FeedVideoCard.vue'

const auth = useAuthStore()
const toast = useToastStore()

const canLike = computed(() => auth.isLoggedIn)

const state = reactive({
  loading: false,
  error: '',
  items: [] as FeedVideoItem[],
  hasMore: false,
  limit: 10,
  asOf: 0,
  nextOffset: 0,
  // 一旦降级到 MySQL 累计热度排序，本次分页会话就固定在该排序上，
  // 避免 Redis 中途恢复时把两套榜单拼在一起。
  degraded: false,
  // DB 降级游标：Redis 不可用时服务端返回 as_of = 0 并改走 MySQL 复合游标，
  // 此时 offset 不再有意义，翻页必须靠这三个字段。
  nextLatestPopularity: 0,
  nextLatestBefore: '',
  nextLatestIDBefore: 0,
})

const likeBusy = reactive<Record<string, boolean>>({})

async function loadHot(reset: boolean) {
  if (state.loading) return
  state.loading = true
  state.error = ''
  try {
    // 只要上一页返回过游标就一直带上：Redis 正常时服务端会忽略它，
    // 一旦中途降级到 MySQL，它就成了唯一能继续翻页的依据。
    const hasCursor = !reset && state.nextLatestIDBefore > 0 && state.nextLatestBefore !== ''
    const res = await feedApi.listByPopularity({
      limit: state.limit,
      as_of: reset ? 0 : state.asOf,
      offset: reset ? 0 : state.nextOffset,
      force_db: !reset && state.degraded,
      latest_popularity: hasCursor ? state.nextLatestPopularity : undefined,
      latest_before: hasCursor ? state.nextLatestBefore : undefined,
      latest_id_before: hasCursor ? state.nextLatestIDBefore : undefined,
    })
    if (res.expired) {
      // as_of 已滑出可翻页窗口：清空会话后重新取第一页，不把错位数据拼进来。
      state.items = []
      state.hasMore = false
      state.asOf = 0
      state.nextOffset = 0
      state.nextLatestPopularity = 0
      state.nextLatestBefore = ''
      state.nextLatestIDBefore = 0
      state.degraded = false
      state.loading = false
      toast.error('榜单已过期，已重新加载')
      await loadHot(true)
      return
    }
    state.hasMore = res.has_more
    state.asOf = res.as_of
    state.nextOffset = res.next_offset
    state.degraded = !!res.degraded
    state.nextLatestPopularity = res.next_latest_popularity ?? 0
    state.nextLatestBefore = res.next_latest_before ?? ''
    state.nextLatestIDBefore = res.next_latest_id_before ?? 0
    state.items = reset ? res.video_list : state.items.concat(res.video_list)
  } catch (e) {
    state.error = e instanceof ApiError ? e.message : String(e)
  } finally {
    state.loading = false
  }
}

async function toggleLike(item: FeedVideoItem) {
  if (!auth.isLoggedIn) {
    toast.error('请先登录')
    return
  }
  const key = String(item.id)
  if (likeBusy[key]) return
  likeBusy[key] = true
  try {
    if (item.is_liked) await likeApi.unlike(item.id)
    else await likeApi.like(item.id)
    item.is_liked = !item.is_liked
    item.likes_count = Math.max(0, item.likes_count + (item.is_liked ? 1 : -1))
  } catch (e) {
    const msg = e instanceof ApiError ? e.message : String(e)
    toast.error(msg)
  } finally {
    likeBusy[key] = false
  }
}

onMounted(async () => {
  await loadHot(true)
})
</script>

<template>
  <div class="card">
    <div class="row" style="justify-content: space-between; align-items: baseline">
      <div>
        <p class="title" style="margin: 0">热榜</p>
        <p class="subtle" style="margin: 6px 0 0">最近一小时互动热度（/feed/listByPopularity）</p>
      </div>

      <div class="row">
        <label class="subtle" style="margin: 0">limit</label>
        <input v-model.number="state.limit" type="number" min="1" max="50" style="width: 90px" :disabled="state.loading" />
        <button class="primary" type="button" :disabled="state.loading" @click="loadHot(true)">刷新</button>
        <button type="button" :disabled="state.loading || !state.hasMore" @click="loadHot(false)">加载更多</button>
      </div>
    </div>

    <div v-if="state.error" class="pill bad" style="margin-top: 12px">错误：{{ state.error }}</div>
    <div v-else-if="state.degraded" class="subtle" style="margin-top: 12px">
      Redis 不可用，已降级为累计热度排序；本次翻页会保持该排序，刷新后恢复窗口热榜。
    </div>
    <div v-else-if="state.loading && state.items.length === 0" class="subtle" style="margin-top: 12px">加载中…</div>
    <div v-else-if="state.items.length === 0" class="subtle" style="margin-top: 12px">暂无内容</div>

    <div v-if="state.items.length" class="rank-list" style="margin-top: 14px">
      <div v-for="(item, idx) in state.items" :key="`hot-${item.id}`" class="rank-row">
        <div class="rank-num" :class="idx < 3 ? 'top' : ''">{{ idx + 1 }}</div>
        <FeedVideoCard
          :item="item"
          :can-like="canLike"
          :busy="!!likeBusy[String(item.id)]"
          @toggle-like="toggleLike"
        />
      </div>
    </div>
  </div>
</template>

<style scoped>
.rank-list {
  display: grid;
  gap: 12px;
}

.rank-row {
  display: grid;
  grid-template-columns: 44px minmax(0, 1fr);
  gap: 12px;
  align-items: start;
}

.rank-num {
  height: 44px;
  width: 44px;
  border-radius: 16px;
  display: grid;
  place-items: center;
  font-weight: 900;
  letter-spacing: 0.2px;
  border: 1px solid rgba(255, 255, 255, 0.12);
  background: rgba(255, 255, 255, 0.06);
  color: rgba(255, 255, 255, 0.86);
  user-select: none;
}

.rank-num.top {
  border-color: rgba(254, 44, 85, 0.55);
  background: rgba(254, 44, 85, 0.18);
  color: rgba(255, 255, 255, 0.96);
}

@media (max-width: 900px) {
  .rank-row {
    grid-template-columns: 38px minmax(0, 1fr);
    gap: 10px;
  }
  .rank-num {
    height: 38px;
    width: 38px;
    border-radius: 14px;
  }
}
</style>

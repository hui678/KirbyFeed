<script setup lang="ts">
import { computed, onMounted, reactive } from 'vue'

import { ApiError } from '../api/client'
import * as feedApi from '../api/feed'
import * as likeApi from '../api/like'
import type { FeedVideoItem, TagSummary } from '../api/types'
import { useAuthStore } from '../stores/auth'
import { useToastStore } from '../stores/toast'
import FeedVideoCard from './FeedVideoCard.vue'

const auth = useAuthStore()
const toast = useToastStore()

const canLike = computed(() => auth.isLoggedIn)

const state = reactive({
  tagsLoading: false,
  tagsError: '',
  tags: [] as TagSummary[],
  activeTag: '',
  loading: false,
  error: '',
  items: [] as FeedVideoItem[],
  hasMore: false,
  nextTime: 0,
  nextIdBefore: undefined as number | undefined,
  limit: 10,
})

const likeBusy = reactive<Record<string, boolean>>({})

async function loadTags() {
  state.tagsLoading = true
  state.tagsError = ''
  try {
    const res = await feedApi.listTags({ limit: 20 })
    state.tags = res.tags ?? []
    const first = state.tags[0]
    if (!state.activeTag && first) {
      state.activeTag = first.name
      await loadVideos(true)
    }
  } catch (e) {
    state.tagsError = e instanceof ApiError ? e.message : String(e)
  } finally {
    state.tagsLoading = false
  }
}

async function loadVideos(reset: boolean) {
  if (!state.activeTag || state.loading) return
  state.loading = true
  state.error = ''
  try {
    const res = await feedApi.listByTag({
      tag_name: state.activeTag,
      limit: state.limit,
      latest_time: reset ? 0 : state.nextTime,
      latest_id_before: reset ? undefined : state.nextIdBefore,
    })
    state.hasMore = res.has_more
    state.nextTime = res.next_time
    state.nextIdBefore = res.next_id_before
    state.items = reset ? res.video_list : state.items.concat(res.video_list)
  } catch (e) {
    state.error = e instanceof ApiError ? e.message : String(e)
  } finally {
    state.loading = false
  }
}

function selectTag(name: string) {
  if (name === state.activeTag && state.items.length > 0) return
  state.activeTag = name
  state.items = []
  state.hasMore = false
  state.nextTime = 0
  state.nextIdBefore = undefined
  void loadVideos(true)
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

onMounted(loadTags)
</script>

<template>
  <div class="card">
    <div class="row" style="justify-content: space-between; align-items: baseline">
      <div>
        <p class="title" style="margin: 0">话题</p>
        <p class="subtle" style="margin: 6px 0 0">按视频数排序的热门标签（/feed/listByTag）</p>
      </div>
      <button type="button" :disabled="state.loading || !state.activeTag" @click="loadVideos(true)">刷新</button>
    </div>

    <div v-if="state.tagsLoading" class="subtle" style="margin-top: 12px">标签加载中…</div>
    <div v-else-if="state.tagsError" class="pill bad" style="margin-top: 12px">标签错误：{{ state.tagsError }}</div>
    <div v-else-if="state.tags.length === 0" class="subtle" style="margin-top: 12px">
      暂无话题。发布视频时在标题或简介里加 #话题，这里会自动出现。
    </div>
    <div v-else class="tag-bar" style="margin-top: 12px">
      <button
        v-for="tag in state.tags"
        :key="tag.name"
        class="tag-chip"
        :class="{ on: tag.name === state.activeTag }"
        type="button"
        @click="selectTag(tag.name)"
      >
        #{{ tag.name }}
        <span class="tag-count">{{ tag.video_count }}</span>
      </button>
    </div>

    <div v-if="state.error" class="pill bad" style="margin-top: 12px">错误：{{ state.error }}</div>
    <div v-else-if="state.loading && state.items.length === 0" class="subtle" style="margin-top: 12px">加载中…</div>
    <div v-else-if="state.activeTag && state.items.length === 0" class="subtle" style="margin-top: 12px">
      该话题下暂无视频
    </div>

    <div v-if="state.items.length" class="topic-list" style="margin-top: 14px">
      <FeedVideoCard
        v-for="item in state.items"
        :key="`topic-${item.id}`"
        :item="item"
        :can-like="canLike"
        :busy="!!likeBusy[String(item.id)]"
        @toggle-like="toggleLike"
      />
    </div>

    <div v-if="state.items.length" class="row" style="justify-content: center; margin-top: 14px">
      <button type="button" :disabled="state.loading || !state.hasMore" @click="loadVideos(false)">
        {{ state.hasMore ? '加载更多' : '没有更多了' }}
      </button>
    </div>
  </div>
</template>

<style scoped>
.tag-bar {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.tag-chip {
  border: 1px solid rgba(255, 255, 255, 0.14);
  background: rgba(255, 255, 255, 0.06);
  color: rgba(255, 255, 255, 0.88);
  border-radius: 999px;
  padding: 7px 12px;
  cursor: pointer;
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-size: 13px;
}

.tag-chip.on {
  border-color: rgba(254, 44, 85, 0.55);
  background: rgba(254, 44, 85, 0.16);
}

.tag-count {
  color: rgba(255, 255, 255, 0.55);
  font-variant-numeric: tabular-nums;
}

.topic-list {
  display: grid;
  gap: 12px;
}
</style>

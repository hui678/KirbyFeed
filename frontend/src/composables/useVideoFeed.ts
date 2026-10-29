import { computed, reactive, ref } from 'vue'
import { ApiError } from '../api/client'
import * as feedApi from '../api/feed'
import type { FeedVideoItem } from '../api/types'
import { useAuthStore } from '../stores/auth'

// hot / topic 由各自的列表面板管理状态，这里只负责可上下滑动的两条视频流。
export type TabKey = 'recommend' | 'following' | 'hot' | 'topic'

export function useVideoFeed() {
  const auth = useAuthStore()
  const tab = ref<TabKey>('recommend')

  const recommend = reactive({
    items: [] as FeedVideoItem[],
    loading: false, error: '',
    hasMore: false, nextTime: 0,
    nextIdBefore: undefined as number | undefined,
  })

  const following = reactive({
    items: [] as FeedVideoItem[],
    loading: false, error: '',
    hasMore: false, nextTime: 0,
    nextIdBefore: undefined as number | undefined,
  })

  const currentState = computed(() => {
    if (tab.value === 'following') return following
    return recommend
  })

  async function loadRecommend(reset: boolean) {
    if (recommend.loading) return
    recommend.loading = true
    recommend.error = ''
    try {
      const res = await feedApi.listLatest({
        limit: 10,
        latest_time: reset ? 0 : recommend.nextTime,
        latest_id_before: reset ? undefined : recommend.nextIdBefore,
      })
      recommend.hasMore = res.has_more
      recommend.nextTime = res.next_time
      recommend.nextIdBefore = res.next_id_before
      recommend.items = reset ? res.video_list : recommend.items.concat(res.video_list)
    } catch (e) {
      recommend.error = e instanceof ApiError ? e.message : String(e)
    } finally {
      recommend.loading = false
    }
  }

  async function loadFollowing(reset: boolean) {
    if (!auth.isLoggedIn) {
      following.error = '登录后才能查看关注流'
      return
    }
    if (following.loading) return
    following.loading = true
    following.error = ''
    try {
      const res = await feedApi.listByFollowing({
        limit: 10,
        latest_time: reset ? 0 : following.nextTime,
        latest_id_before: reset ? undefined : following.nextIdBefore,
      })
      following.hasMore = res.has_more
      following.nextTime = res.next_time
      following.nextIdBefore = res.next_id_before
      following.items = reset ? res.video_list : following.items.concat(res.video_list)
    } catch (e) {
      following.error = e instanceof ApiError ? e.message : String(e)
    } finally {
      following.loading = false
    }
  }

  async function ensureTabLoaded() {
    if (tab.value === 'recommend' && recommend.items.length === 0) await loadRecommend(true)
    if (tab.value === 'following' && following.items.length === 0) await loadFollowing(true)
  }

  async function loadMoreIfNeeded(activeIndex: number) {
    const items = currentState.value.items
    if (items.length === 0) return
    if (activeIndex < items.length - 3) return
    if (tab.value === 'recommend' && recommend.hasMore) await loadRecommend(false)
    if (tab.value === 'following' && following.hasMore) await loadFollowing(false)
  }

  return { tab, recommend, following, currentState, loadRecommend, loadFollowing, ensureTabLoaded, loadMoreIfNeeded }
}

import { postJson } from './client'
import { normalizeFeedVideoList } from './normalize'
import type { ListByFollowingResponse, ListByPopularityResponse, ListLatestResponse, ListLikesCountResponse } from './types'

export async function listLatest(input: { limit: number; latest_time: number; latest_id_before?: number }) {
  const body: Record<string, unknown> = { limit: input.limit, latest_time: input.latest_time }
  // 复合游标：只有同时给出 id 才能唯一定位游标点，否则同毫秒的数据会被跳过
  if (typeof input.latest_id_before === 'number' && input.latest_id_before > 0) {
    body.latest_id_before = input.latest_id_before
  }
  const res = await postJson<ListLatestResponse>('/feed/listLatest', body)
  return { ...res, video_list: normalizeFeedVideoList(res.video_list) }
}

export async function listLikesCount(input: { limit: number; likes_count_before?: number; id_before?: number }) {
  const body: Record<string, unknown> = { limit: input.limit }
  if (typeof input.likes_count_before === 'number' || typeof input.id_before === 'number') {
    body.likes_count_before = input.likes_count_before ?? 0
    body.id_before = input.id_before ?? 0
  }
  const res = await postJson<ListLikesCountResponse>('/feed/listLikesCount', body)
  return { ...res, video_list: normalizeFeedVideoList(res.video_list) }
}

export async function listByPopularity(input: {
  limit: number
  as_of: number
  offset: number
  latest_popularity?: number
  latest_before?: string
  latest_id_before?: number
}) {
  const body: Record<string, unknown> = {
    limit: input.limit,
    as_of: input.as_of,
    offset: input.offset,
  }
  // DB 降级游标（Redis 不可用时服务端走 MySQL 复合游标翻页）。
  // 三个字段必须同时出现，缺任何一个后端都会判定为「未提供游标」并拒绝翻页。
  if (
    typeof input.latest_popularity === 'number' &&
    typeof input.latest_before === 'string' &&
    typeof input.latest_id_before === 'number'
  ) {
    body.latest_popularity = input.latest_popularity
    body.latest_before = input.latest_before
    body.latest_id_before = input.latest_id_before
  }
  const res = await postJson<ListByPopularityResponse>('/feed/listByPopularity', body)
  return { ...res, video_list: normalizeFeedVideoList(res.video_list) }
}

export async function listByFollowing(input: { limit: number; latest_time: number; latest_id_before?: number }) {
  const body: Record<string, unknown> = { limit: input.limit, latest_time: input.latest_time }
  if (typeof input.latest_id_before === 'number' && input.latest_id_before > 0) {
    body.latest_id_before = input.latest_id_before
  }
  const res = await postJson<ListByFollowingResponse>('/feed/listByFollowing', body, { authRequired: true })
  return { ...res, video_list: normalizeFeedVideoList(res.video_list) }
}

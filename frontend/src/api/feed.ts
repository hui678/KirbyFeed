import { postJson } from './client'
import { normalizeFeedVideoList } from './normalize'
import type {
  ListByFollowingResponse,
  ListByPopularityResponse,
  ListByTagResponse,
  ListLatestResponse,
  ListTagsResponse,
} from './types'

export async function listLatest(input: { limit: number; latest_time: number; latest_id_before?: number }) {
  const body: Record<string, unknown> = { limit: input.limit, latest_time: input.latest_time }
  // 复合游标：只有同时给出 id 才能唯一定位游标点，否则同毫秒的数据会被跳过
  if (typeof input.latest_id_before === 'number' && input.latest_id_before > 0) {
    body.latest_id_before = input.latest_id_before
  }
  const res = await postJson<ListLatestResponse>('/feed/listLatest', body)
  return { ...res, video_list: normalizeFeedVideoList(res.video_list) }
}

export async function listTags(input: { limit?: number } = {}) {
  return postJson<ListTagsResponse>('/feed/listTags', { limit: input.limit ?? 20 })
}

export async function listByTag(input: {
  tag_name: string
  limit: number
  latest_time: number
  latest_id_before?: number
}) {
  const body: Record<string, unknown> = {
    tag_name: input.tag_name,
    limit: input.limit,
    latest_time: input.latest_time,
  }
  // 与 listLatest 相同的复合游标：只给时间会在同毫秒数据上漏项。
  if (typeof input.latest_id_before === 'number' && input.latest_id_before > 0) {
    body.latest_id_before = input.latest_id_before
  }
  const res = await postJson<ListByTagResponse>('/feed/listByTag', body)
  return { ...res, video_list: normalizeFeedVideoList(res.video_list) }
}

export async function listByPopularity(input: {
  limit: number
  as_of: number
  offset: number
  force_db?: boolean
  latest_popularity?: number
  latest_before?: string
  latest_id_before?: number
}) {
  const body: Record<string, unknown> = {
    limit: input.limit,
    as_of: input.as_of,
    offset: input.offset,
  }
  // 已降级的分页会话固定走 MySQL 累计热度排序：Redis 中途恢复也不会换榜，
  // 否则同一个会话会把两套排序拼在一起，出现重复/跳变。
  if (input.force_db) {
    body.force_db = true
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

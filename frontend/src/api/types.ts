export type MessageResponse = { message: string }

export type DirectMessage = {
  id: number
  from_id: number
  to_id: number
  content: string
  is_read: boolean
  created_at: string
}

export type ListMessagesResponse = {
  messages: DirectMessage[]
}

export type TokenResponse = { token: string; refresh_token?: string; account_id?: number; username?: string }

export type Account = {
  id: number
  username: string
  avatar_url?: string
  bio?: string
}

export type Video = {
  id: number
  author_id: number
  username: string
  title: string
  description?: string
  play_url: string
  cover_url: string
  create_time: string
  likes_count: number
}

export type Comment = {
  id: number
  username: string
  video_id: number
  author_id: number
  content: string
  created_at: string
}

export type FeedAuthor = {
  id: number
  username: string
}

export type FeedVideoItem = {
  id: number
  author: FeedAuthor
  title: string
  description?: string
  play_url: string
  cover_url: string
  create_time: number
  likes_count: number
  is_liked: boolean
}

export type ListLatestResponse = {
  video_list: FeedVideoItem[]
  next_time: number
  next_id_before?: number
  has_more: boolean
}

export type TagSummary = {
  name: string
  video_count: number
}

export type ListTagsResponse = {
  tags: TagSummary[]
}

export type ListByTagResponse = {
  video_list: FeedVideoItem[]
  next_time: number
  next_id_before?: number
  has_more: boolean
}

export type ListByPopularityResponse = {
  video_list: FeedVideoItem[]
  as_of: number
  next_offset: number
  has_more: boolean
  // 来自 MySQL 累计热度榜（Redis 不可用），不是窗口热榜
  degraded?: boolean
  // as_of 已滑出可翻页窗口，需要从第一页重新开始
  expired?: boolean
  next_latest_popularity?: number
  next_latest_before?: string
  next_latest_id_before?: number
}

export type ListByFollowingResponse = {
  video_list: FeedVideoItem[]
  next_time: number
  next_id_before?: number
  has_more: boolean
}

export type IsLikedResponse = {
  is_liked: boolean
}

export type GetAllFollowersResponse = {
  followers: Account[]
}

export type GetAllVloggersResponse = {
  vloggers: Account[]
}

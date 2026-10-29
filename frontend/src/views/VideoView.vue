<script setup lang="ts">
import { onUnmounted, reactive, ref, watch } from 'vue'
import { RouterLink, useRouter } from 'vue-router'

import AppShell from '../components/AppShell.vue'
import { ApiError } from '../api/client'
import * as videoApi from '../api/video'
import type { Video } from '../api/types'
import { useAuthStore } from '../stores/auth'
import { useToastStore } from '../stores/toast'
import SparkMD5 from 'spark-md5'

const router = useRouter()
const auth = useAuthStore()
const toast = useToastStore()

const busy = ref(false)
const stage = ref('')
const published = ref<Video | null>(null)

const videoInput = ref<HTMLInputElement | null>(null)
const coverInput = ref<HTMLInputElement | null>(null)

const publishForm = reactive({
  title: '',
  description: '',
  video: null as File | null,
  cover: null as File | null,
})

const preview = reactive({
  videoUrl: '',
  coverUrl: '',
})

// chunk upload progress
const uploadProgress = reactive({
  uploadedBytes: 0,
  totalBytes: 0,
  percent: 0,
})

function setPreviewVideo(file: File | null) {
  if (preview.videoUrl) URL.revokeObjectURL(preview.videoUrl)
  preview.videoUrl = file ? URL.createObjectURL(file) : ''
}

function setPreviewCover(file: File | null) {
  if (preview.coverUrl) URL.revokeObjectURL(preview.coverUrl)
  preview.coverUrl = file ? URL.createObjectURL(file) : ''
}

watch(
  () => publishForm.video,
  (f) => setPreviewVideo(f),
)

watch(
  () => publishForm.cover,
  (f) => setPreviewCover(f),
)

onUnmounted(() => {
  setPreviewVideo(null)
  setPreviewCover(null)
})

function pickVideo(e: Event) {
  const input = e.target as HTMLInputElement
  publishForm.video = input.files?.[0] ?? null
}

function pickCover(e: Event) {
  const input = e.target as HTMLInputElement
  publishForm.cover = input.files?.[0] ?? null
}

function openVideoPicker() {
  videoInput.value?.click()
}

function openCoverPicker() {
  coverInput.value?.click()
}

function clearVideo() {
  publishForm.video = null
  if (videoInput.value) videoInput.value.value = ''
}

function clearCover() {
  publishForm.cover = null
  if (coverInput.value) coverInput.value.value = ''
}

function resetProgress() {
  uploadProgress.uploadedBytes = 0
  uploadProgress.totalBytes = 0
  uploadProgress.percent = 0
}

// Compute file md5 by reading in 2MB chunks
async function computeFileMD5(file: File): Promise<string> {
  const chunkSize = 2 << 20
  const spark = new SparkMD5.ArrayBuffer()
  for (let offset = 0; offset < file.size; offset += chunkSize) {
    const end = Math.min(offset + chunkSize, file.size)
    const buf = await file.slice(offset, end).arrayBuffer()
    spark.append(buf)
  }
  return spark.end()
}

// Compute md5 for a single chunk blob
async function computeChunkMD5(blob: Blob): Promise<string> {
  const buf = await blob.arrayBuffer()
  const spark = new SparkMD5.ArrayBuffer()
  spark.append(buf)
  return spark.end()
}

const CHUNK_SIZE = 5 << 20 // 5 MB，与后端 video.ChunkSize 对齐
const MAX_CONCURRENT = 3
const MAX_RETRIES = 3
const CHUNK_TIMEOUT_MS = 120_000 // 单片 5MB，弱网下也该在 2 分钟内给出结果
const MAX_RESUME_ROUNDS = 3 // 合并前最多按服务端位图补传几轮

function sleep(ms: number) {
  return new Promise<void>((r) => setTimeout(r, ms))
}

// 只有「可能是暂时性故障」的错误才重试：4xx 说明请求本身不对，重试也不会变好
function isFatal(e: unknown): boolean {
  if (e instanceof ApiError) {
    if (e.status === 401 || e.status === 408 || e.status === 409) return false
    return e.status >= 400 && e.status < 500
  }
  return false
}

// 服务端在合并时发现「位图说传了、磁盘上没有」的分片，会回一个缺失清单。
// 只有把这几片重新传一次，这个会话才不会一路卡到 TTL 过期。
function missingChunkIndices(e: unknown): number[] {
  if (!(e instanceof ApiError) || !e.payload || typeof e.payload !== 'object') return []
  const raw = (e.payload as { missing_chunks?: unknown }).missing_chunks
  if (!Array.isArray(raw)) return []
  return raw.filter((i): i is number => Number.isInteger(i) && (i as number) >= 0)
}

// 整文件 MD5 与 init 声明不一致时，服务端会作废会话（避免客户端对着坏数据无限重试），
// 此时重传旧 upload_id 只会 404，必须重新 init 走完整流程。
function isFileHashMismatch(e: unknown): boolean {
  if (!(e instanceof ApiError) || !e.payload || typeof e.payload !== 'object') return false
  return (e.payload as { code?: unknown }).code === 'file_hash_mismatch'
}

function chunkBytes(chunkIndex: number, file: File): number {
  const start = chunkIndex * CHUNK_SIZE
  return Math.min(start + CHUNK_SIZE, file.size) - start
}

async function uploadVideoChunked(file: File, restartRound = 0): Promise<videoApi.UploadResponse> {
  const totalChunks = Math.ceil(file.size / CHUNK_SIZE)
  // SparkMD5 是同步计算，会占住主线程；先让浏览器把提示画出来再算
  stage.value = '计算文件指纹'
  await sleep(0)
  const fileHash = await computeFileMD5(file)

  stage.value = '初始化上传'
  const initRes = await videoApi.initChunkUpload({
    filename: file.name,
    file_size: file.size,
    chunk_size: CHUNK_SIZE,
    total_chunks: totalChunks,
    file_hash: fileHash,
  })

  const uploadId = initRes.upload_id
  // 断点续传：init 命中的就是服务端位图里已置位的分片，直接跳过
  const done = new Set<number>(initRes.uploaded_chunks)

  uploadProgress.totalBytes = file.size
  const syncProgress = () => {
    let bytes = 0
    for (let i = 0; i < totalChunks; i++) {
      if (done.has(i)) bytes += chunkBytes(i, file)
    }
    uploadProgress.uploadedBytes = bytes
    uploadProgress.percent = file.size > 0 ? Math.round((bytes / file.size) * 100) : 0
  }
  syncProgress()

  const uploadOne = async (chunkIndex: number): Promise<void> => {
    const start = chunkIndex * CHUNK_SIZE
    const end = Math.min(start + CHUNK_SIZE, file.size)
    const blob = file.slice(start, end)
    const chunkHash = await computeChunkMD5(blob)

    let lastErr: unknown
    for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
      try {
        await videoApi.uploadChunk(uploadId, chunkIndex, chunkHash, blob, CHUNK_TIMEOUT_MS)
        done.add(chunkIndex)
        syncProgress()
        return
      } catch (e) {
        lastErr = e
        if (isFatal(e)) break
        // 指数退避 + jitter：避免同一瞬间的失败请求一起重试，把服务端再打一遍
        if (attempt < MAX_RETRIES - 1) {
          await sleep(300 * 2 ** attempt + Math.random() * 200)
        }
      }
    }
    throw lastErr
  }

  // 并发池：同一时刻最多 MAX_CONCURRENT 个分片在飞
  const runPool = async (indices: number[]): Promise<void> => {
    let idx = 0
    await new Promise<void>((resolve, reject) => {
      let active = 0
      let stopped = false

      const next = () => {
        if (stopped) return
        if (idx >= indices.length && active === 0) {
          resolve()
          return
        }
        while (active < MAX_CONCURRENT && idx < indices.length) {
          const ci = indices[idx++] as number
          active++
          uploadOne(ci)
            .then(() => { active--; next() })
            .catch((e) => { stopped = true; reject(e) })
        }
      }
      next()
    })
  }

  const missingChunks = (uploaded: number[]): number[] => {
    const set = new Set(uploaded)
    const gaps: number[] = []
    for (let i = 0; i < totalChunks; i++) {
      if (!set.has(i)) gaps.push(i)
    }
    return gaps
  }

  const pending = missingChunks(initRes.uploaded_chunks)
  if (pending.length > 0) {
    stage.value = '上传视频'
    await runPool(pending)
  }

  // 合并前用服务端位图核对一次：并发或重试中「服务端成功但响应丢了」的分片，
  // 在这里按缺口补传，而不是让 complete 直接 400。
  for (let round = 0; round < MAX_RESUME_ROUNDS; round++) {
    stage.value = '核对分片'
    const st = await videoApi.chunkStatus(uploadId)
    // 以服务端位图为准：本地记账可能因「服务端成功但响应丢了」而不全
    st.uploaded_chunks.forEach((i) => done.add(i))
    syncProgress()
    const gaps = missingChunks(st.uploaded_chunks)
    if (gaps.length === 0) break
    stage.value = `补传分片（第 ${round + 1} 轮，缺 ${gaps.length} 片）`
    await runPool(gaps)
  }

  stage.value = '合并文件'
  let lastErr: unknown
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      return await videoApi.completeChunkUpload(uploadId)
    } catch (e) {
      lastErr = e
      // 409 = 服务端正在合并（并发 complete），稍等后重试即可，不是失败
      if (e instanceof ApiError && e.status === 409) {
        await sleep(1000 * (attempt + 1))
        continue
      }
      // 整文件校验失败：服务端已作废会话并清掉映射，旧 upload_id 再传只会 404。
      // 重新 init 完整走一遍；只重试一次，避免客户端指纹本身算错时无限循环。
      if (isFileHashMismatch(e)) {
        if (restartRound >= 1) throw e
        stage.value = '文件校验失败，重新上传'
        return uploadVideoChunked(file, restartRound + 1)
      }
      // 位图声称已传的分片在磁盘上并不存在：把这几片从本地记账里抹掉并立刻补传，
      // 下一轮再合并。少了这一步，这个会话会一直卡到 24h TTL 过期。
      const missing = missingChunkIndices(e)
      if (missing.length > 0) {
        missing.forEach((i) => done.delete(i))
        syncProgress()
        stage.value = `补传丢失的分片（${missing.length} 片）`
        await runPool(missing)
        continue
      }
      throw e
    }
  }
  throw lastErr
}

async function onPublish() {
  if (busy.value) return
  if (!auth.isLoggedIn) {
    toast.error('请先登录')
    await router.push('/account')
    return
  }

  const title = publishForm.title.trim()
  const description = publishForm.description.trim()
  if (!title) {
    toast.error('请输入 title')
    return
  }
  if (!publishForm.video) {
    toast.error('请选择视频文件（.mp4）')
    return
  }
  if (!publishForm.cover) {
    toast.error('请选择封面图片（jpg/png/webp）')
    return
  }

  busy.value = true
  stage.value = ''
  published.value = null
  resetProgress()
  try {
    const videoRes = await uploadVideoChunked(publishForm.video!)

    stage.value = '上传封面'
    const coverRes = await videoApi.uploadCover(publishForm.cover!)

    const coverUrl = coverRes.url || coverRes.cover_url || ''
    const playUrl = videoRes.url || videoRes.play_url || ''
    if (!coverUrl || !playUrl) {
      toast.error('上传成功但缺少 url')
      return
    }

    stage.value = '发布视频'
    const res = await videoApi.publishVideo({ title, description, play_url: playUrl, cover_url: coverUrl })

    published.value = res
    toast.success('已发布')

    publishForm.title = ''
    publishForm.description = ''
    clearVideo()
    clearCover()
  } catch (e) {
    const msg = e instanceof ApiError ? e.message : String(e)
    toast.error(msg)
  } finally {
    busy.value = false
    stage.value = ''
    resetProgress()
  }
}
</script>

<template>
  <AppShell>
    <div class="publish-wrap">
      <div class="card publish-card">
        <div class="row" style="justify-content: space-between; align-items: baseline">
          <p class="title" style="margin: 0">发布视频</p>
          <div v-if="busy" class="pill">进行中：{{ stage || '…' }}</div>
        </div>
        <p class="subtle" style="margin-top: 10px">选择视频文件与封面图片，上传到本机后自动生成 URL，再写入 `/video/publish`。</p>

        <div class="grid form-grid" style="margin-top: 16px">
          <div>
            <label>title</label>
            <input v-model.trim="publishForm.title" class="big-input" :disabled="busy" />
          </div>
          <div>
            <label>description</label>
            <textarea v-model.trim="publishForm.description" class="big-input" :disabled="busy" />
          </div>
          <div class="grid two">
            <div>
              <label>video (.mp4)</label>
              <input ref="videoInput" class="file-native" type="file" accept="video/mp4" :disabled="busy" @change="pickVideo" />
              <div class="file-box">
                <button type="button" :disabled="busy" @click="openVideoPicker">选择视频</button>
                <div class="file-name" :class="publishForm.video ? '' : 'muted'">
                  {{ publishForm.video ? publishForm.video.name : '未选择文件' }}
                </div>
                <button v-if="publishForm.video" type="button" :disabled="busy" @click="clearVideo">清除</button>
              </div>
              <div v-if="publishForm.video" class="subtle" style="margin-top: 6px">
                已选择：{{ publishForm.video.name }}（{{ Math.ceil(publishForm.video.size / 1024 / 1024) }} MB）
              </div>
            </div>
            <div>
              <label>cover (jpg/png/webp)</label>
              <input
                ref="coverInput"
                class="file-native"
                type="file"
                accept="image/jpeg,image/png,image/webp"
                :disabled="busy"
                @change="pickCover"
              />
              <div class="file-box">
                <button type="button" :disabled="busy" @click="openCoverPicker">选择封面</button>
                <div class="file-name" :class="publishForm.cover ? '' : 'muted'">
                  {{ publishForm.cover ? publishForm.cover.name : '未选择文件' }}
                </div>
                <button v-if="publishForm.cover" type="button" :disabled="busy" @click="clearCover">清除</button>
              </div>
              <div v-if="publishForm.cover" class="subtle" style="margin-top: 6px">已选择：{{ publishForm.cover.name }}</div>
            </div>
          </div>

          <!-- Upload progress bar -->
          <div v-if="busy && uploadProgress.totalBytes > 0" class="progress-wrap">
            <div class="progress-bar">
              <div class="progress-fill" :style="{ width: uploadProgress.percent + '%' }"></div>
            </div>
            <div class="progress-text">
              {{ (uploadProgress.uploadedBytes / 1024 / 1024).toFixed(1) }} MB /
              {{ (uploadProgress.totalBytes / 1024 / 1024).toFixed(1) }} MB
              ({{ uploadProgress.percent }}%)
            </div>
          </div>

          <div v-if="preview.coverUrl || preview.videoUrl" class="grid two">
            <div v-if="preview.videoUrl" class="preview-card">
              <div class="subtle">视频预览</div>
              <video class="video" :src="preview.videoUrl" controls playsinline preload="metadata" />
            </div>
            <div v-if="preview.coverUrl" class="preview-card">
              <div class="subtle">封面预览</div>
              <img class="cover" :src="preview.coverUrl" alt="cover preview" />
            </div>
          </div>

          <div class="row" style="justify-content: flex-end; margin-top: 8px">
            <button class="primary big-btn" type="button" :disabled="busy" @click="onPublish">发布</button>
          </div>
        </div>

        <div v-if="published" class="card" style="margin-top: 14px">
          <p class="title">已发布</p>
          <div class="row" style="justify-content: space-between">
            <div>
              <div class="title" style="margin: 0">{{ published.title }}</div>
              <div class="subtle mono">#{{ published.id }}</div>
            </div>
            <div class="row">
              <RouterLink class="pill" :to="`/video/${published.id}`">去播放</RouterLink>
              <a class="pill mono" :href="published.play_url" target="_blank" rel="noreferrer">play_url</a>
              <a class="pill mono" :href="published.cover_url" target="_blank" rel="noreferrer">cover_url</a>
            </div>
          </div>
        </div>
      </div>
    </div>
  </AppShell>
</template>

<style scoped>
.publish-wrap {
  display: grid;
  justify-items: center;
}

.publish-card {
  width: min(980px, 100%);
  padding: 22px;
}

.form-grid {
  gap: 16px;
}

.form-grid .grid.two {
  gap: 20px;
}

.form-grid .grid.two > * {
  min-width: 0;
}

.form-grid input[type='file'] {
  max-width: 100%;
}

.file-native {
  position: absolute;
  width: 1px;
  height: 1px;
  padding: 0;
  margin: -1px;
  overflow: hidden;
  clip: rect(0, 0, 0, 0);
  white-space: nowrap;
  border: 0;
}

.file-box {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 10px 12px;
  border: 1px solid rgba(255, 255, 255, 0.12);
  background: rgba(255, 255, 255, 0.06);
  border-radius: 14px;
  min-height: 46px;
}

.file-box button {
  padding: 8px 10px;
  border-radius: 12px;
}

.file-name {
  flex: 1;
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-size: 13px;
  color: rgba(255, 255, 255, 0.88);
}

.muted {
  color: rgba(255, 255, 255, 0.55);
}

.big-input {
  box-sizing: border-box;
  width: 100%;
  max-width: 100%;
  padding: 12px 14px;
  font-size: 14px;
  border-radius: 14px;
}

.big-btn {
  padding: 12px 18px;
  font-size: 14px;
  border-radius: 14px;
}

.preview-card {
  border: 1px solid rgba(255, 255, 255, 0.12);
  background: rgba(255, 255, 255, 0.05);
  border-radius: 16px;
  padding: 12px;
  display: grid;
  gap: 10px;
}

.cover {
  width: 100%;
  aspect-ratio: 9/12;
  object-fit: cover;
  border-radius: 14px;
  border: 1px solid rgba(255, 255, 255, 0.1);
  background: rgba(0, 0, 0, 0.35);
}

.video {
  width: 100%;
  border-radius: 14px;
  border: 1px solid rgba(255, 255, 255, 0.1);
  background: rgba(0, 0, 0, 0.35);
}

.progress-wrap {
  display: grid;
  gap: 6px;
}

.progress-bar {
  height: 8px;
  background: rgba(255, 255, 255, 0.1);
  border-radius: 4px;
  overflow: hidden;
}

.progress-fill {
  height: 100%;
  background: #4a9eff;
  border-radius: 4px;
  transition: width 0.2s ease;
}

.progress-text {
  font-size: 13px;
  color: rgba(255, 255, 255, 0.7);
}
</style>

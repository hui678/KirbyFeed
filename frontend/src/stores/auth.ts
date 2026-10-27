import { defineStore } from 'pinia'
import { computed, ref } from 'vue'

import { decodeJwtPayload, type JwtPayload } from '../utils/jwt'

const ACCESS_KEY = 'access_token'
const REFRESH_KEY = 'refresh_token'

function readStored(key: string): string | null {
  try { return localStorage.getItem(key) } catch { return null }
}

function writeStored(key: string, value: string) {
  try { localStorage.setItem(key, value) } catch {}
}

function removeStored(key: string) {
  try { localStorage.removeItem(key) } catch {}
}

export const useAuthStore = defineStore('auth', () => {
  const token = ref<string | null>(readStored(ACCESS_KEY))
  const refreshToken = ref<string | null>(readStored(REFRESH_KEY))

  // 时钟：access token 只有 15 分钟，UI 侧的登录态必须随时间推进而失效。
  // 这里只是「提前把过期表现出来」的体验优化，真正的校验永远在后端。
  const nowSec = ref(Math.floor(Date.now() / 1000))
  if (typeof window !== 'undefined') {
    window.setInterval(() => {
      nowSec.value = Math.floor(Date.now() / 1000)
    }, 30_000)
  }

  const claims = computed<JwtPayload | null>(() => (token.value ? decodeJwtPayload(token.value) : null))
  const isExpired = computed(() => {
    const exp = claims.value?.exp
    if (typeof exp !== 'number') return false // 解析不出来就交给后端判，不在前端误杀
    return nowSec.value >= exp
  })

  // 修复要点：旧实现只判断「token 字符串是否存在」，过期 token 仍显示已登录，
  // 路由守卫也照常放行，直到下一次 API 返回 401 才被清理。
  const isLoggedIn = computed(() => !!token.value && !isExpired.value)

  function setToken(newToken: string) {
    token.value = newToken
    writeStored(ACCESS_KEY, newToken)
  }

  function setTokens(access: string, refresh: string) {
    token.value = access
    refreshToken.value = refresh
    writeStored(ACCESS_KEY, access)
    writeStored(REFRESH_KEY, refresh)
  }

  function clearTokens() {
    token.value = null
    refreshToken.value = null
    removeStored(ACCESS_KEY)
    removeStored(REFRESH_KEY)
  }

  return { token, refreshToken, isLoggedIn, isExpired, claims, setToken, setTokens, clearTokens }
})

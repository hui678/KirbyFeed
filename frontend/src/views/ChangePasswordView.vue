<script setup lang="ts">
import { computed, reactive, ref } from 'vue'
import { useRouter } from 'vue-router'

import AppShell from '../components/AppShell.vue'
import { ApiError } from '../api/client'
import * as accountApi from '../api/account'
import { useToastStore } from '../stores/toast'
import { useAuthStore } from '../stores/auth'

const router = useRouter()
const toast = useToastStore()
const auth = useAuthStore()

const busy = ref(false)
const form = reactive({ oldPassword: '', newPassword: '' })

const needLogin = computed(() => !auth.isLoggedIn)

async function submit() {
  if (busy.value) return
  const oldPassword = form.oldPassword
  const newPassword = form.newPassword
  if (!oldPassword || !newPassword) {
    toast.error('请把信息填完整')
    return
  }

  busy.value = true
  try {
    await accountApi.changePassword(oldPassword, newPassword)
    // 后端改密成功后会吊销 access/refresh，本地残留的 token 已经没用了，直接清掉。
    auth.clearTokens()
    toast.success('密码已修改，请重新登录')
    await router.push('/account')
  } catch (e) {
    const msg = e instanceof ApiError ? e.message : String(e)
    toast.error(msg)
  } finally {
    busy.value = false
  }
}
</script>

<template>
  <AppShell>
    <div class="grid two">
      <div class="card">
        <p class="title">修改密码</p>
        <p class="subtle">需要登录（账号身份来自 access token）。</p>
        <div v-if="needLogin" class="muted" style="margin-top: 12px">当前未登录，请先到「账号」页登录。</div>
        <div v-else class="grid" style="margin-top: 12px">
          <div>
            <label>old_password</label>
            <input v-model="form.oldPassword" type="password" autocomplete="current-password" />
          </div>
          <div>
            <label>new_password</label>
            <input v-model="form.newPassword" type="password" autocomplete="new-password" />
          </div>
          <div class="row" style="justify-content: flex-end">
            <button class="primary" type="button" :disabled="busy" @click="submit">提交</button>
          </div>
        </div>
      </div>

      <div class="card">
        <p class="title">提示</p>
        <p class="muted">改密成功后后端会同时吊销 access token 与 refresh token，需要重新登录。</p>
      </div>
    </div>
  </AppShell>
</template>

import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// 开发态由 Vite 代理到本机 8000；生产态由 nginx 代理到 api 容器。
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': 'http://localhost:8000',
    },
  },
})

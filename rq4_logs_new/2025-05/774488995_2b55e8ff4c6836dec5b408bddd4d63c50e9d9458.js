import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import fs from 'fs'

export default defineConfig({
  build:{
    outDir : '../Backend/wwwroot',
  },
  plugins: [react()],
  server: {
    port: 3000,
    proxy: {
      '/api': {
        target: 'https://localhost:7098',
        changeOrigin: true,
        secure: false,
      }
    }
  }
})
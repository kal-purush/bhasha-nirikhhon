import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import svgr from 'vite-plugin-svgr'

// https://vitejs.dev/config/
export default defineConfig({
  base: '/hjelpemidler/formidler/',
  plugins: [react(), svgr()],
  server: {
    proxy: {
      '/hjelpemidler/formidler/api': {
        target: 'http://localhost:5000',
        changeOrigin: true,
      },
      '/hjelpemidler/formidler/soknad-api': {
        target: 'http://localhost:5000',
        changeOrigin: true,
      },
    },
  },
})
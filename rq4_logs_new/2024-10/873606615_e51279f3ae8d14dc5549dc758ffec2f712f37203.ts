import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import dts from 'vite-plugin-dts'
import path from 'path'

export default defineConfig({
  plugins: [react(), dts({ insertTypesEntry: true })],
  build: {
    lib: {
      entry: path.resolve(__dirname, 'src/index.ts'), // Asosiy kirish fayli
      name: 'ReactEimzoSolijonovmuhammadali',
      formats: ['es', 'cjs'], // ESM va CJS formatlar
      fileName: (format) => `react-eimzo-solijonovmuhammadali.${format}.js` // Paketning fayl nomi
    },
    rollupOptions: {
      input: {
        Eimzo: path.resolve(__dirname, './Eimzo.js'),
        eimzo: path.resolve(__dirname, './e-imzo.js'),
        eimzoClient: path.resolve(__dirname, './e-imzo-client.js')
      },
      external: ['react', 'react-dom'], // React va ReactDOM tashqi modullar
      output: {
        globals: {
          react: 'React',
          'react-dom': 'ReactDOM'
        }
      }
    }
  }
})
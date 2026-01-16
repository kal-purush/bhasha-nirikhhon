import react from '@vitejs/plugin-react';
import * as path from 'path';
import { defineConfig } from 'vite';

// https://vitejs.dev/config/

/**
 * Для рендеринга приложения на сервере.
 * Собираем фактически JS библиотеку.
 *
 * Использовать на сервере его не можем, потому его результатом является бандл для SPA.
 * На сервере бандл для SPA (vite.config.js) мы не можем использовать потому что:
 * - На сервере нет точки входа для JS – `index.html`
 * - В бандле для SPA идет обращение к `react-dom/client`
 * - В бандле для SPA по умолчанию используются ES6 импорты, которые Node.js не понимает. Ноде надо CommonJS модули, иначе работать не будет.
 */
module.exports = defineConfig({
  build: {
    /**
     * Устанавливаем опцию lib чтобы сказать, что будем собирать наше приложение как внешнюю библиотеку.
     * То есть как пакет, который можно будет подключить в другой пакет, как будто бы из NPM registry.
     * Будем использовать этот пакет в пакете с сервером.
     */
    lib: {
      entry: path.resolve(__dirname, 'ssr.tsx'),
      /**
       * Не играет роли.
       */
      name: 'Client',
      /**
       * Так как результат сборки планируем использовать в окружении Node.js, указываем CommonJS-модули.
       * Например, ES6-модули по умолчанию не будут работать в Node.js.
       */
      formats: ['cjs'],
    },
    minify: false,
    rollupOptions: {
      output: {
        dir: 'dist-ssr',
      },
    },
  },
  plugins: [react()],
  resolve: {
    alias: {
      '@api': path.resolve(__dirname, 'src', 'api'),
      '@assets': path.resolve(__dirname, 'src', 'assets'),
      '@components': path.resolve(__dirname, 'src', 'components'),
      '@constants': path.resolve(__dirname, 'src', 'constants'),
      '@pages': path.resolve(__dirname, 'src', 'pages'),
      '@store': path.resolve(__dirname, 'src', 'store'),
      '@app-types': path.resolve(__dirname, 'src', 'app-types'),
      '@utils': path.resolve(__dirname, 'src', 'utils'),
      '@src': path.resolve(__dirname, 'src'),
    },
  },
});
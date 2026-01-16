import { defineConfig } from 'cypress'

export default defineConfig({
  projectId: '',
  e2e: {
    baseUrl: 'http://localhost:3000',
    fileServerFolder: '.',
    specPattern: '**/*.cy.{js,jsx,ts,tsx}',
    modifyObstructiveCode: false,
    supportFile: './src/support/index.ts',
    video: false,
    videoUploadOnPasses: false,
    videosFolder: '../../dist/cypress/apps/wallet-mock/videos',
    screenshotsFolder: '../../dist/cypress/apps/wallet-mock/screenshots',
    chromeWebSecurity: false,
    viewportWidth: 1440,
    viewportHeight: 900,
  },
  env: {
    environment: 'CUSTOM',
    commitHash: 'dev',
  },
})
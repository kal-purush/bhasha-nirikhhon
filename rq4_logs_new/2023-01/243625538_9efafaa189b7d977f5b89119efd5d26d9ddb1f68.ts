import { devices, PlaywrightTestConfig } from '@playwright/test'
import path from 'path'

const PORT = process.env.PORT || 3000
const baseURL = `http://localhost:${PORT}`

const config: PlaywrightTestConfig = {
  timeout: 30 * 1000,
  testDir: path.join(__dirname, 'tests'),
  testMatch: /.*(test|spec)\.(ts|tsx|js|jsx)/,
  retries: 2,
  outputDir: 'tests/results/',
  webServer: {
    command: 'npm run dev',
    url: baseURL,
    timeout: 120 * 1000,
    reuseExistingServer: false,
  },
  use: {
    baseURL,
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
      fullyParallel: true,
    },
    {
      name: 'chromium-mobile',
      use: { ...devices['Pixel 5'] },
      fullyParallel: true,
    },
    {
      name: 'webkit-mobile',
      use: { ...devices['iPhone 12'] },
      fullyParallel: true,
    },
    {
      name: 'chromium-dark',
      use: { ...devices['Desktop Chrome'], colorScheme: 'dark' },
      fullyParallel: true,
    },
    {
      name: 'chromium-mobile-dark',
      use: { ...devices['Pixel 5'], colorScheme: 'dark' },
      fullyParallel: true,
    },
    {
      name: 'webkit-mobile-dark',
      use: { ...devices['iPhone 12'], colorScheme: 'dark' },
      fullyParallel: true,
    },
  ],
  reporter: [['junit', { outputFile: 'results.xml' }]],
}

export default config
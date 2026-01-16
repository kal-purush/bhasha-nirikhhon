import { expect, test } from '@playwright/test'
import axios from 'axios'

test.describe('splitBillApp', () => {
  test.beforeEach(async ({ page }) => {
    await axios.get('http://localhost:3000/init')
    await page.goto('http://localhost:3001')
  })
})
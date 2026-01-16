import { test, expect } from '@playwright/test'

test.beforeEach(async ({ page }) => {
    page.on('console', (msg) => {
        try {
            switch (msg.type()) {
                case 'error': console.error(msg.text()); break;
                case 'warning': console.warn(msg.text()); break;
                default: console.log(msg.text()); break;
            }
        } catch (e) {
            // better silently ignore as console.log might cause the flakyness
        }
    })
    await page.goto('/')
    await expect(page.getByText('Drag \'n\' drop files here, or click to select files')).toBeVisible()
    const inputFile = page.locator('input[type="file"]')
    await inputFile.waitFor({ state: 'hidden' })

    await page.setInputFiles('input[type="file"]', './basic_test.trf', { strict: true })
})

test('can select pkg summary', async ({ page }) => {
    const locTreeView = page.getByTestId('workbench.section.treeView')
    const locTreePkgSmokeTest = locTreeView.getByText('SmokeTest')
    await locTreePkgSmokeTest.click()
    await expect(page.getByTestId('trfPkgSummaryView')).toBeVisible()
})

test('can select pkg test cases  with list view', async ({ page }) => {
    const locTreeView = page.getByTestId('workbench.section.treeView')
    const locTreePkgSmokeTestExtendIcon = locTreeView.locator('css=div.MuiTreeItem-iconContainer').nth(1) // first one is the project one
    await locTreePkgSmokeTestExtendIcon.click()
    const locTreeTestCases = locTreeView.getByText('Test case')
    await locTreeTestCases.click()
    await expect(page.getByTestId('trfListView')).toBeVisible()
})
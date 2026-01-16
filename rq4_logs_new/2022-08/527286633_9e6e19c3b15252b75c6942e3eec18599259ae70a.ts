import * as puppeteer from 'puppeteer';

const URL: string = 'https://platzi.com/'

describe('Extracting information', () => {

  let browser: puppeteer.Browser
  let page: puppeteer.Page

  beforeAll( async () => {
    browser = await puppeteer.launch({
      defaultViewport: null,
      slowMo: 500
    })

    page = await browser.newPage()
  })

  afterAll( async () => {
    await browser.close()
  })

  it('Should extract title from page', async () => {
    await page.goto(URL, {
      waitUntil: 'networkidle2'
    })

    const title = await page.title()
    const pageUrl = await page.url()

    expect(title).toContain('Cursos Online Profesionales de Tecnología')
    expect(pageUrl).toBe(URL)

  }, 350000)

  it('Should extract info from element', async () => {
    await page.goto(URL, {
      waitUntil: 'networkidle2'
    })

    await page.waitForSelector('#home-public-old > header > nav > div.Actionsv2 > a')

    const buttonName = await page.$eval('#home-public-old > header > nav > div.Actionsv2 > a', (button) => button.textContent)
    const [loginButton] = await page.$x('//*[@id="home-public-old"]/header/nav/div[4]/div/a')
    const loginButtonProperty = await loginButton.getProperty('textContent')
    const loginButtonText = await loginButtonProperty.jsonValue()

    const loginButtonTxt = await page.evaluate((name) => name.textContent, loginButton)

    expect(buttonName).toEqual('Empresas')
    expect(loginButtonText).toEqual('Iniciar sesión')
    expect(loginButtonTxt).toEqual(loginButtonText)


  }, 350000)

  it('Should count all page elements', async () => {
    await page.goto(URL, {
      waitUntil: 'networkidle2'
    })

    const imagesLength = await page.$$eval('img', (images) => images.length)

    expect(imagesLength).toBeGreaterThan(100)


  }, 350000)

})
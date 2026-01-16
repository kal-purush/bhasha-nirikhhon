const HtmlWebpackPlugin = require('html-webpack-plugin')

const path = require('path') // 系统路径模块
const fs = require('fs') // 文件模块

function readPage () {
  const file = path.join(__dirname, 'page.json')
  let pages = null
  const data = fs.readFileSync(file, 'utf-8')
  if (!data) {
    console.error('page.json读取失败')
  } else {
    pages = JSON.parse(data)
  }
  return pages
}
function getEntry (pages) {
  const entry = {}
  Object.keys(pages).forEach(key => {
    entry[key] = pages[key].js
  })
  return entry
}

function getHtmlPlugin (pages) {
  const html = []
  Object.keys(pages).forEach(key => {
    const p = new HtmlWebpackPlugin({ template: pages[key].html, chunks: [key], filename: `${key}.html` })
    html.push(p)
  })
  return html
}

const pages = readPage()
module.exports = {
  entry: getEntry(pages),
  htmlPlugin: getHtmlPlugin(pages)
}
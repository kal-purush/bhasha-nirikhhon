import app from 'commander'
import info from './package.json'

function dimension(val) {
  return parseInt(val, 10);
}

app
  .version(info.version)
  .usage('[options] <url ...>')
  .option('-w, --width <n>', 'set width of screenshot', dimension, 640)
  .option('-h, --height <n>', 'set height of screenshot', dimension, 384)
  .option('-s --stylesheet-url <n>', 'set to inject optional stylesheet before rendering the page')
  .parse(process.argv)

import Renderbit from './index'
const renderer = new Renderbit()

async function handleAll(arr, fn) {
  for(const item of arr) await fn(item)
}

handleAll(app.args, url => {
  return renderer.render(url, {
    width: app.width,
    height: app.height,
    stylesheetUrl: app.stylesheetUrl
  })
})
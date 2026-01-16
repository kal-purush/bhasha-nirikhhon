process.removeAllListeners('warning')

import gm from 'gm'
import { QuestionId, Question } from './src/types'
import * as Utils from './src/image-utils'

const srcImagePath = './assets/card-1200x630.png'
const baseDir = './data/images'

const progressBar = Utils.createProgressBar()

const generatePreviewImages = async ({
  id,
  lang,
  question,
}: Question): Promise<unknown> => {
  return new Promise((resolve) => {
    gm.subClass({ imageMagick: true })(srcImagePath) // background
      .gravity('NorthWest') // start position for text
      .background('none') // text background
      .fill('#F5F5F5') // text color
      .font(Utils.getFont(lang === 'zh')) // text font
      .out('-size', '1100x500', 'caption:' + question) // text in rectangle
      .out('-geometry', '+10+25') // adjust text rectangle
      .gravity('North') // place text on image
      .out('-composite') // mode for placing text
      // .write(`test/${lang}-${id}.png`, (e) => {
      .write(`${baseDir}/${lang}/${id}.png`, (e) => {
        progressBar.increment()
        resolve(e)
      })
  })
}

async function generateForQuestion(id: QuestionId = '1') {
  const promises = Utils.getAllQuestions()
    .filter((q) => q.id === id)
    .map((q) => generatePreviewImages(q))

  await Promise.all(promises)
}

async function generateAllImages() {
  const questions = Utils.getAllQuestions()
  const ids = Utils.getAllQuestionIds()

  progressBar.start(questions.length, 0)

  for (let id = 0; id < ids.length; id++) {
    await generateForQuestion(ids[id])
  }
}

const main = async () => {
  const start = performance.now()
  Utils.resetLanguageDirs(baseDir)
  await generateAllImages()

  // await generateImages1Question()
  progressBar.stop()
  // generatePreviewImages('1', 'da')
  const end = performance.now()
  console.log(`\n\nExecution time: ${Math.round((end - start) / 1000)}s`)
}

await main()
import {
  pagesToArray,
  getNumFromString,
  getDateFromString,
  mapLimit
} from './utils'
import store from '../store'
import { getDom } from './crawler'
const COUNT_15 = 15
const SAW_COLUMN = [
  { header: '游戏', key: 'game', width: 32 },
  { header: '链接', key: 'link', width: 46 },
  { header: '简评', key: 'comment', width: 10 },
  { header: '评分', key: 'rank', width: 10 },
  { header: '日期', key: 'date', width: 10 }
]
const WISH_COLUMN = [
  { header: '游戏', key: 'game', width: 32 },
  { header: '链接', key: 'link', width: 46 },
  { header: '日期', key: 'date', width: 10 }
]
/**
 *获取已玩游戏
 * @param {String} url
 * @param {Number} id
 * @returns {Array}
 */
export async function getSaw(id) {
  let pages = pagesToArray(store.state.base.game.collect, COUNT_15)
  return await mapLimit(pages, async (page, callback) => {
    let url = `https://www.douban.com/people/${id}/games?action=collect&start=${page}`
    getDom(url).then($ => {
      let _arr = []
      $('.game-list .content').each((_, item) => {
        _arr.push({
          game: $(item)
            .find('.title a')
            .text(),
          link: $(item)
            .find('.title a')
            .attr('href'),
          comment: $(item)
            .find(':nth-child(3)')
            .text()
            .trim(),
          rank:
            getNumFromString(
              $(item)
                .find('.rating-star')
                .attr('class')
            ) / 10,
          date: getDateFromString(
            $(item)
              .find('.date')
              .text()
          )
        })
      })
      callback(null, _arr)
    })
  })
}
/**
 *获取想玩游戏
 * @param {String} url
 * @param {Number} id
 * @returns {Array}
 */
async function getWish(id) {
  let pages = pagesToArray(store.state.base.book.wish, COUNT_15)
  return await mapLimit(pages, async (page, callback) => {
    let url = `https://www.douban.com/people/${id}/games?action=wish&start=${page}`
    getDom(url).then($ => {
      let _arr = []
      $('.game-list .content').each((_, item) => {
        _arr.push({
          game: $(item)
            .find('.title a')
            .text(),
          link: $(item)
            .find('.title a')
            .attr('href'),
          date: getDateFromString(
            $(item)
              .find('.date')
              .text()
          )
        })
      })
      callback(null, _arr)
    })
  })
}

/**
 * @param {Number} id
 * @returns {Array}
 */
export const getGames = async id => {
  return [await getSaw(id), await getWish(id)]
}
/**
 * 游戏数据写入excel
 * @param {Number} id
 * @param {*} workbook
 */
export const gameToExcel = async (id, workbook) => {
  let [saw, wish] = await getGames(id)
  let sheetSaw = workbook.addWorksheet('游戏-玩过'),
    sheetWish = workbook.addWorksheet('游戏-想玩')
  sheetSaw.columns = SAW_COLUMN
  sheetWish.columns = WISH_COLUMN

  sheetSaw.addRows(saw)
  sheetWish.addRows(wish)
}
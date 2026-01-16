function format_string(str, ...arguments) {
  return str.replace(/{(\d+)}/g, (match, number) => {
    return typeof arguments[number] != 'undefined'
      ? ("" + arguments[number]).trim()
      : match
  });
};

const form = document.getElementById("form");
const resultArea = document.getElementById("result-area");

const mangaTemplate = [
  '@article{{0}{3}{1}},',
  '  author  = {{0}},',
  '  title   = {{1}},',
  '  journal = {{2}},',
  '  year    = {{3}},',
  '  month   = {{4}},',
  '  volume  = {{5}},',
  '  pages   = {{6}-{7}}',
  '}',
].join('\r\n')

const doujinTemplate = [
  '@misc{{5}_{1},',
  '  author    = {{0}},',
  '  title     = {{1}},',
  '  year      = {{2}},',
  '  month     = {{3}},',
  '  day       = {{4}},',
  '  publisher = {{5}}',
  '}',
].join('\r\n')

function generate() {
  const formData = new FormData(form)

  const type = formData.get('type')

  const name = formData.get('name')
    .replace(/\s*\[中国.*\]/g, '')
    .replace(/\[DL版\]\s*/, '')
    .replace('コミック\s*', 'COMIC ')

  const dateInfo = {
    'doujin': [],
    'manga': name.match(/(\d+)年(\d+)月号/),
  }[type] || []

  const year = dateInfo[1] || formData.get('year')
  const month = dateInfo[2] || formData.get('month')
  const day = formData.get('date')

  const matching = name.match({
    'doujin': /\[(.*?)(\(.*\))?\]\s*(.*)/,
    'manga': /\[(.*?)\](.*?)\((.*)\s*(.*年.*月号)*?\)/,
  }[type])

  if (!matching)
    return false

  const volume = formData.get('volume')
  const pageStart = formData.get('pageStart')
  const pageEnd = formData.get('pageEnd')

  const result = {
    'doujin': format_string(doujinTemplate, matching[1].replaceAll(/\s/g, ''), matching[3], year, month, day, (matching[2]||'').replace(/^\s*?\(/, '').replace(/\)\s*?$/, '').replaceAll(/\s/g, '')),
    'manga': format_string(mangaTemplate, matching[1], matching[2], matching[3].replace(/\d+年\d+月号/, ''), year, month, volume, pageStart, pageEnd),
  }[type].replaceAll(/\r\n.*(null|\{)\},?/g, '')

  resultArea.textContent = result
}
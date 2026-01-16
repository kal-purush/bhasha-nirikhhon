export const generateLog = (input, output, url) => {
  const header = { 'content-type': 'application/json;charset=utf-8' }

  wx.request({
    url: 'http://192.168.10.104:8000/log',
    method: 'POST',
    data: {
      input,
      output,
      url,
    },
    header: header,
  })
}
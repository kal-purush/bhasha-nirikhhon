interface Ajax4UploadOptions extends AjaxOptions<FormData> {
  requestList?: XMLHttpRequest[]
}

interface AjaxOptions<T> {
  url: string
  onUploadProgress?: XMLHttpRequestEventTarget['onprogress']
  method?: 'POST' | 'GET' | 'PUT'
  data?: T
  headers?: Record<string, string>
}

export function ajax4Upload(
  {
    method = 'GET',
    url,
    data,
    headers = {},
    requestList,
    onUploadProgress,
  }: Ajax4UploadOptions,
) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest()
    xhr.upload.onprogress = function (ev) {
      // console.log('onprogress', ev)
      onUploadProgress?.call(this, ev)
    }
    xhr.onprogress = function (ev) {
      // console.log('onprogress', ev)
    }
    xhr.onload = () => {
      // 将请求成功的 xhr 从列表中删除
      if (requestList) {
        const xhrIndex = requestList.findIndex(item => item === xhr)
        xhrIndex !== -1 && requestList.splice(xhrIndex, 1)
      }
      if (xhr.status === 200) {
        const response = xhr.response
        // 特殊逻辑，和后端配合
        if (response === 'true')
          resolve(response)
        else
          reject(response)
      }
      else {
        // eslint-disable-next-line prefer-promise-reject-errors
        reject({ status: xhr.status, res: xhr.response })
      }
    }

    // 用户取消
    xhr.onabort = () => {
      reject(new Error('Cancel'))
    }
    // 网络错误
    xhr.onerror = (err) => {
      reject(err)
    }
    Object.keys(headers).forEach(key =>
      xhr.setRequestHeader(key, headers[key]),
    )
    xhr.open(method, url, true)
    xhr.send(data)
  })
}

export function ajax({ method = 'GET', url, data, headers = {} }: AjaxOptions<any>) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest()
    xhr.responseType = 'json'

    xhr.onprogress = function (ev) {
      console.log('onprogress', ev)
    }

    // xhr.upload.onprogress = function (ev) {
    //   console.log('upload onprogress', ev)
    //   onprogress?.call(this, ev)
    // }

    xhr.onload = () => {
      if (xhr.status === 200) {
        const response = xhr.response
        resolve(response)
      }
      else {
        // eslint-disable-next-line prefer-promise-reject-errors
        reject({ status: xhr.status, res: xhr.response })
      }
    }
    // 网络错误
    xhr.onerror = (err) => {
      reject(err)
    }
    xhr.open(method, url)

    Object.keys(headers).forEach(key =>
      xhr.setRequestHeader(key, headers[key]),
    )

    xhr.send(data)
  })
}
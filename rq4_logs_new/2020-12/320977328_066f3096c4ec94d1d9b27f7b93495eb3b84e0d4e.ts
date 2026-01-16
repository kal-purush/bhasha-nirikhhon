

export const uploadToGyazo = async() => {
  const FormData = require('form-data');
  const fetch = require('node-fetch');
  const formData = new FormData();
  formData.append('client_id', '4466b5521d01325cff8e619e41f3dd1e6c051fda3c91bee198cd8e2a4d0a0983');
  formData.append('referer_url', 'https://gyazo.com');
  const imgPath = 'puppeteer/tmp.png'

  const imagedata = await openImage(imgPath);
  formData.append('image_url', imagedata);
  
  await fetch('https://upload.gyazo.com/api/upload/easy_auth', {
    method: 'POST',
    body: formData
  }).then((response) => {
    if (response.status !== 200) {
      console.log('there was a problem. Status Code: ' + response.status)
    }
  },(e) => {
    console.log('Fetch error'+ e)
  });
  console.log('it\'s all done')
}

function openImage (imgPath: string) {
  return new Promise((resolve,reject) => {
    const fs = require('fs')
    fs.readFile(imgPath, {encoding: 'base64'}, (e,data) => {
      if (e) {
        console.log(e)
        reject(e)
      } else {
        console.log('suceeded in reading an image')
        resolve(data)
      }
    })
  })
};
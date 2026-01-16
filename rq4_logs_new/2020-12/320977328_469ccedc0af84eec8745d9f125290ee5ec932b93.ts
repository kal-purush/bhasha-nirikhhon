

export const uploadToGyazo = () => {
  const FormData = require('form-data');
  const fetch = require('node-fetch');
  const formData = new FormData();
  const img = '/puppeteer/tmp.png'
  const imagedata = imageToBase64(img)
  formData.append('imagedata', imagedata);
  formData.append('access_token', 'a0fddb4984de822e0bc4a90bd7021980f9c516ea7c86e523f3b8495339ccc82a');
  fetch('https://upload.gyazo.com/api/upload', {
    method: 'POST',
    body: formData 
  });
}

const imageToBase64 = (imagePath) => {
  const image = new Image();
  image.src = imagePath
  const canvas = document.createElement('canvas');
  canvas.setAttribute('width', String(image.width));
  canvas.setAttribute('width', String(image.height));
  canvas.getContext('2d')?.drawImage(image, 0, 0);
  return canvas.toDataURL("image/png");
}
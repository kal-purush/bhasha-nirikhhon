import './style.css'
import typescriptLogo from './typescript.svg'
import viteLogo from '/vite.svg'
import { setupCounter } from './counter.ts'
// import { encode } from "../../../pkg"
import { encode } from 'wasm-avif';

(async function main() {
  const pic = new Image()
  pic.src = "./photo.jpg"
  await new Promise(resolve => {
    pic.onload = resolve
  })
  const canvas = document.getElementById("canvas") as HTMLCanvasElement;
  const width = 720
  canvas.width = width; // destination canvas size
  canvas.height = canvas.width * pic.height / pic.width;
  const ctx = canvas.getContext("2d") as CanvasRenderingContext2D;

  //simple, canvas downscalling
  // ctx.drawImage(pic, 0, 0, canvas.width, canvas.height)


  // stepped scalling
  // var oc = document.createElement('canvas'),
  //         octx = oc.getContext('2d')!;

  // var cur = {
  //   width: Math.floor(pic.width * 0.5),
  //   height: Math.floor(pic.height * 0.5)
  // }

  // oc.width = cur.width;
  // oc.height = cur.height;

  // octx.drawImage(pic, 0, 0, cur.width, cur.height);

  // while (cur.width * 0.5 > width) {
  //   cur = {
  //     width: Math.floor(cur.width * 0.5),
  //     height: Math.floor(cur.height * 0.5)
  //   };
  //   octx.drawImage(oc, 0, 0, cur.width * 2, cur.height * 2, 0, 0, cur.width, cur.height);
  // }
  // ctx.drawImage(oc, 0, 0, cur.width, cur.height, 0, 0, canvas.width, canvas.height);




  // oc.width = Math.floor(pic.width * 0.5);
  // oc.height = Math.floor(pic.height * 0.5);
  // octx.drawImage(pic, 0, 0, oc.width, oc.height);
  // octx.drawImage(oc, 0, 0, oc.width * 0.5, oc.height * 0.5);
  // ctx.drawImage(oc, 0, 0, oc.width * 0.5, oc.height * 0.5,
  //   0, 0, canvas.width, canvas.height);

  pic.height = width * pic.height / pic.width;
  pic.width = width
  ctx.drawImage(pic, 0, 0, pic.width, pic.height)


  // ctx.scale(0.5, 0.5)
  const imageData = ctx.getImageData(0, 0, canvas.width, canvas.height)
  const avif = encode(new Uint8Array(imageData.data.buffer), imageData.width, imageData.height)
  const blob = new Blob([avif], {type: "image/avif"})
  const file = new File([blob], 'untitled', { type: blob.type })
  const objectURL = URL.createObjectURL(file);
  console.log('url ', objectURL)

  const image = document.getElementById("img") as HTMLImageElement
  image.src = objectURL
  // console.log( encode(imageData.data, imageData.width, imageData.height))
})()

document.querySelector<HTMLDivElement>('#app')!.innerHTML = `
  <div>
    <a href="https://vitejs.dev" target="_blank">
      <img src="${viteLogo}" class="logo" alt="Vite logo" />
    </a>
    <a href="https://www.typescriptlang.org/" target="_blank">
      <img src="${typescriptLogo}" class="logo vanilla" alt="TypeScript logo" />
    </a>
    <h1>Vite + TypeScript</h1>
    <div class="card">
      <button id="counter" type="button"></button>
    </div>
    <p class="read-the-docs">
      Click on the Vite and TypeScript logos to learn more
    </p>
  </div>
`

setupCounter(document.querySelector<HTMLButtonElement>('#counter')!)
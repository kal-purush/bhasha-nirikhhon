import { ImgData } from '../cnst/images'

function _getHeigth (
  images: ImgData[],
  containerWidth: number,
  margin: number,
): number {
  const width = containerWidth - images.length * margin
  const r = images.reduce((r, img) => r + img.fullw / img.fullh, 0)
  return width / r // have to round down because Firefox will automatically roundup value with number of decimals > 3
}

function _setHeight (images: ImgData[], height: number): void {
  images.forEach(img => {
    img.w = height * img.fullw / img.fullh
    img.h = height
  })
}

function deepCopy<T> (o: T): T {
  return JSON.parse(JSON.stringify(o))
}

/**
 * Mutates imgs: adds .w, .h
 */
export function doImageLayout (
  containerWidth: number,
  maxHeight: number,
  margin: number,
  imgs: ImgData[],
): void {
  let imgNodes = imgs.slice(0)

  w: while (imgNodes.length > 0) {
    let slice: ImgData[]
    let h: number

    for (let i = 1; i <= imgNodes.length; i++) {
      slice = imgNodes.slice(0, i)
      h = _getHeigth(slice, containerWidth, margin)

      if (h < maxHeight) {
        _setHeight(slice, h)
        imgNodes = imgNodes.slice(i)
        continue w
      }
    }

    _setHeight(slice, Math.min(maxHeight, h))
    break
  }
}
'use strict'

const arrowsEl = Array.from(document.querySelectorAll('.slider__arrow'))

const picturesEl = Array.from(document.querySelectorAll('.slider__item'))
console.log(picturesEl)

let maxPicInd = picturesEl.length - 1
let activePicInd = 0

function setActiveImg(ind) {
  picturesEl.forEach((pic) => {
    pic.classList.remove('slider__item_active')
  })
  picturesEl[ind].classList.add('slider__item_active')
}

arrowsEl.forEach((arrow) => {
  arrow.addEventListener('click', () => {
    if (arrow.classList.contains('slider__arrow_prev')) {
      activePicInd -= 1
      if (activePicInd < 0) {
        activePicInd = maxPicInd
      }
    } else {
      activePicInd += 1
      if (activePicInd > maxPicInd) {
        activePicInd = 0
      }
    }
    setActiveImg(activePicInd)
  })
})
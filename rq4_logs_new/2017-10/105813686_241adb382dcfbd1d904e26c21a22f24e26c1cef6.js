console.log('loaded')
let shouldResize = false;
let video = document.querySelector('.video-player')

function getScroll() {
  let scroll = document.documentElement.scrollTop
  scroll > 1 ? (shouldResize = true) : (shouldResize = false)
  console.log(shouldResize)
  shouldResize ? video.classList.add('resized') : video.classList.remove('resized');
}

document.addEventListener('scroll', getScroll)
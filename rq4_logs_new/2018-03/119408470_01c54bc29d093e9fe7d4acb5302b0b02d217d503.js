const progressBar = document.getElementById('progress');
progressBar.max = document.body.offsetHeight;
const vh = window.innerHeight;

export function setProgress(scrollY) {
  const currentPostion = scrollY || window.scrollY;
  progressBar.value = currentPostion + vh
}
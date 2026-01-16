export function searching(arr, search) {
  search.oninput = function() {
    let value = this.value.toLowerCase().trim()
    if (value !== '') {
      arr.forEach(i => {
        if (i.innerText.toLowerCase().search(value) == -1) {
          i.parentNode.classList.add('hide')
        }
      })
    } else {
      arr.forEach(i => {
        i.parentNode.classList.remove('hide')
      })
    }
  }
}
const item = document.querySelector('.item')

item.addEventListener('dragstart')
item.addEventListener('dragend')


function dragstart(event) {
  console.log('drag start', event.target)
}

function dragend() {
  console.log('drag end')
}
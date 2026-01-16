const body = document.querySelector('body')
let main = document.createElement('main')
body.prepend(main)

img = document.createElement('img')
img.src = "./panda.jpg"
main.appendChild(img)

let a = document.createElement('a')
a.href = " https://www.worldwildlife.org/species/giant-panda "
a.append("Save the Pandas!")
main.appendChild(a)
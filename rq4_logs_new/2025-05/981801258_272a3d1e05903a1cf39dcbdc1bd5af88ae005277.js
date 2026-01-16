const menuBar = document.getElementById("menuBar")
const closeMenu = document.getElementById("close")
const menu = document.getElementById("menu")

const openbtn = document.getElementById("openBtn")
const closeRooms = document.getElementById("shut")
const rooms = document.getElementById("RoomsPage")

menuBar.addEventListener("click", () => {
    menu.classList.add("isactive")
})

closeMenu.addEventListener("click", () => {
    menu.classList.remove("isactive")
})

openbtn.onclick = () => {
    rooms.classList.add("isopen")
}

closeRooms.onclick = () => {
    rooms.classList.remove("isopen")
}
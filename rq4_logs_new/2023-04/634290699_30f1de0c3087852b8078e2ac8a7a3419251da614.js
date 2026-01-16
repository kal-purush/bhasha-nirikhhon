let boxes = document.querySelectorAll(".box.active")
let markBtn = document.querySelector(".mark-btn")
let msgNumbers = document.querySelector(".msg-length")

msgNumbers.innerHTML = boxes.length

boxes.forEach(box => {
    box.onmouseover = () => {
        if (msgNumbers.innerHTML > "0" && box.classList.contains("active")) {
            msgNumbers.innerHTML--
        }
        box.classList.remove("active")
    }
})

markBtn.onclick = (e) => {
    boxes.forEach(box => {
        box.classList.remove("active")
        msgNumbers.innerHTML = "0"
    })
}

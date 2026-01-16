var i = 0;
const srcImg = [
    'assets/images/slide/img_1.png',
    'assets/images/slide/img_2.png',
    'assets/images/slide/img_3.png',
    'assets/images/slide/img_4.png',
    'assets/images/slide/img_5.png'
]

const itemImg = document.getElementById("container_img")
const img = document.createElement("img")
img.id = "imgSlide"
img.src = srcImg[i]

itemImg.appendChild(img)

const prevSlide = document.getElementById("btn_carousel_left")
const nextSlide = document.getElementById("btn_carousel_right")

prevSlide.onclick = () => {
    if( i == 0 )
        i = (srcImg.length)

    document.getElementById("imgSlide").classList.add("removeSlide_prev")
    img.src = srcImg[i]
    
    i--

    const fakeImg = document.createElement("img")
    fakeImg.id = "fakeImg_prev"
    fakeImg.src = srcImg[i]
    itemImg.append(fakeImg)

    setTimeout(() => {
        img.src = srcImg[i]
        img.classList.remove("removeSlide_prev")
        fakeImg.remove()
    }, 900  )
    
}

nextSlide.onclick = () => {
    if( i == (srcImg.length - 1) )
        i = -1

    i++
    
    img.src = srcImg[i]
}
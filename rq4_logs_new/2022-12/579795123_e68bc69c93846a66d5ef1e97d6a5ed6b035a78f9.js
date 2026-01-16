const wrapper = document.querySelector(".slide_wrapper")
const menuItems = document.querySelectorAll(".menu_item")

menuItems.forEach((item,index) => {
    item.addEventListener("click", () => {
        wrapper.style.transform = `translateX(${-100 * index}vw)`;
    });
});

const products = [
    {
        id: 1,
        title: "FENDER STRATOCASTER",
        price: 1999,
        colors: [
            {
                code: "rgb(49, 107, 75)",
                img: "./img/strat2.png",
            },
            {
                code: "rgb(117, 190, 250)",
                img: "./img/strat3.png",
            },
        ],
    },
    {
        id: 2,
        title: "FENDER JAZZMASTER",
        price: 2999,
        colors: [
            {
                code: "white",
                img: "./img/jazz2.png",
            },
            {
                code: "rgb(69, 116, 69)",
                img: "./img/jazz3.png",
            }
        ],
    },
    {
        id: 3,
        title: "FENDER TELECASTER",
        price: 3999,
        colors: [
            {
                code: "black",
                img: "./img/tele2.png"
            },
            {
                code: "yellow",
                img: "./img/tele3.png"
            }
        ]
    },
    {
        id: 4,
        title: "IBANEZ FastShred3000",
        price: 4299,
        colors: [
            {
                code: "rgb(67, 127, 161)",
                img: "./img/ibanez1.png"
            },
            {
                code: "rgb(138, 47, 103)",
                img: "./img/ibanez2.png"
            }
        ]
    },
    {
        id: 5,
        title: "GRETSCH WHITE FALCON",
        price: 9999,
        colors: [
            {
                code: "white",
                img: "./img/gretsch2.png"
            },
            {
                code: "darkblue",
                img: "./img/gretsch3.png"
            }
        ]
    },

]

let choosenProduct = products[0]
const buy_buttons = document.querySelectorAll(".buy_button")
const title = document.querySelector(".product_title")
const price = document.querySelector(".product_price")
const photo = document.querySelector(".product_img")
const colors = document.querySelectorAll(".color")

buy_buttons.forEach((item, index) => {
    item.addEventListener("click", () => {
        title.textContent = products[index].title
        price.textContent = "Price: " + products[index].price + ".00 $"
        photo.src = products[index].colors[0].img;

        colors.forEach((color, index1) => {
            color.style.backgroundColor = products[index].colors[index1].code;

        choosenProduct = products[index]
        });
    });
});

colors.forEach((color, index) => {
    color.addEventListener("click", () => {
        photo.src = choosenProduct.colors[index].img
    })
})

const product_button = document.querySelector(".product_button")
const payment = document.querySelector(".payment")
const close = document.querySelector(".close")

product_button.addEventListener("click", () => {
    payment.style.display = "flex";
})

close.addEventListener("click", () => {
    payment.style.display = "none"
})
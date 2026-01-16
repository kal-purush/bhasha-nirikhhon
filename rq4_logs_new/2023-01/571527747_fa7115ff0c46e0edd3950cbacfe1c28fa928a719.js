let vue = new Vue({
  el: "#wrapper",
  data: {
    shoes: [
      {
        name: "Red Jordan",
        priceThrough: "Rp. 9.000.000,-",
        price: "5000000",
        img: "./images/shoes/shoes-1.png",
        id: 1,
      },
      {
        name: "Grey Jordan",
        priceThrough: "Rp. 12.000.000,-",
        price: "10000000",
        img: "./images/shoes/shoes-2.jfif",
        id: 2,
      },
      {
        name: "White Jordan",
        priceThrough: "Rp. 9.000.000,-",
        price: "5000000",
        img: "./images/shoes/shoes-3.jfif",
        id: 3,
      },
      {
        name: "Black-white Jordan",
        priceThrough: "Rp. 10.000.000,-",
        price: "7000000",
        img: "./images/shoes/shoes-4.jfif",
        id: 4,
      },
      {
        name: "Blue Jordan",
        priceThrough: "Rp. 18.000.000,-",
        price: "16000000",
        img: "./images/shoes/shoes-5.jfif",
        id: 5,
      },
      {
        name: "White-black Jordan",
        priceThrough: "Rp. 20.000.000,-",
        price: "19000000",
        img: "./images/shoes/shoes-6.jfif",
        id: 6,
      },
      {
        name: "Blue-black Jordan",
        priceThrough: "Rp. 5.000.000,-",
        price: "3500000",
        img: "./images/shoes/shoes-7.jfif",
        id: 7,
      },
      {
        name: "Green Jordan",
        priceThrough: "Rp. 8.000.000,-",
        price: "3500000",
        img: "./images/shoes/shoes-8.jfif",
        id: 8,
      },
      {
        name: "Yellow Jordan",
        priceThrough: "Rp. 7.000.000,-",
        price: "3500000",
        img: "./images/shoes/shoes-9.jfif",
        id: 9,
      },
    ],
    details: {
      name: "Red Jordan",
      priceThrough: "Rp. 5.000.000,-",
      price: "3500000",
      img: "./images/shoes/shoes-7.jfif",
    },
    sizeList: [35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50],
    qty: 1,
    packing: "normal",
    size: 35,
    cart: [],
    req: "-",
    total: 0,
    totalQty: 0,
    totalDetail: 0,
    detailDisplay: "none",
    cartDisplay: "none",
    textWA: "",
  },

  methods: {
    changeDetails(id) {
      let shoe = this.shoes.find((item) => item.id === id);
      this.details = shoe;
      this.detailDisplay = "flex";
    },
    incQty() {
      this.qty += 1;
    },
    decQty() {
      this.qty -= 1;
      if (this.qty < 0) {
        this.qty = 0;
      }
    },
    changeSize(event) {
      this.size = event.target.value;
    },
    changePacking(input) {
      this.packing = input;
    },
    addReq(event) {
      this.req = event.target.value;
    },
    addCart() {
      this.cart.push({
        qty: this.qty,
        packing: this.packing,
        size: this.size,
        name: this.details.name,
        price: this.details.price,
        req: this.req,
        img: this.details.img,
        id: +new Date(),
      });
      this.total += +this.details.price * +this.qty;
      this.totalQty += +this.qty;
      this.resetValue();
    },
    removeCartItem(id, qty, price) {
      this.cart = this.cart.filter((item) => item.id !== id);
      this.totalQty -= qty;
      this.total -= +price * +qty;
    },
    resetValue() {
      this.qty = 1;
      this.packing = "normal";
      this.size = 35;
      this.req = "-";
    },
    buyDetail() {
      this.totalDetail = +this.details.price * +this.qty;
      window.location.href =
        "https://api.whatsapp.com/send?phone=6281558903315&text=Halo%20saya%20ingin%20memesan%0Aproduk%3A%20" +
        this.details.name +
        "%0AUkuran%3A%20" +
        this.size +
        "%0AJumlah%3A%20" +
        this.qty +
        "%0ASpecial%20request%3A%20" +
        this.req +
        "%0APackaging%3A%20" +
        this.packing +
        "%0ATotal%3A%20" +
        this.totalDetail +
        "%0A%0ATerimakasih!";
      this.resetValue();
    },
    buyCart() {
      for (const i in this.cart) {
        this.totalDetail += +this.cart[i].price * +this.cart[i].qty;
        this.textWA +=
          "%0Aproduk%3A%20" +
          this.cart[i].name +
          "%0AUkuran%3A%20" +
          this.cart[i].size +
          "%0AJumlah%3A%20" +
          this.cart[i].qty +
          "%0ASpecial%20request%3A%20" +
          this.cart[i].req +
          "%0APackaging%3A%20" +
          this.cart[i].packing +
          "%0A";
      }
      window.location.href =
        "https://api.whatsapp.com/send?phone=6281558903315&text=Halo%20saya%20ingin%20memesan%0A" +
        this.textWA +
        "%0ATotal%3A%20" +
        this.totalDetail +
        "%0A%0ATerimakasih!";
      this.cart = [];
      this.totalQty = 0;
    },
    convertIDR(price) {
      return (rupiah = price
        .toString()
        .replace(/(\d)(?=(\d\d\d)+(?!\d))/g, "$1."));
    },
  },
});
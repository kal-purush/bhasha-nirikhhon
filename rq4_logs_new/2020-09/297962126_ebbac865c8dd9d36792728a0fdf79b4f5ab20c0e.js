let merch = [
  {
    id: 1,
    name: "Summer Gym And Workout Outfits For Men 01",
    size: "XL",
    color: "White",
    price: 2500,
    image: "https://raw.githubusercontent.com/kevinbhingaradiya/LetsUpgrade-Javascript/master/Day%207-8/img/01.jpg",
    description: "Best Summer Gym And Workout Outfits For Men 01 #men #outfits #UrbanMenOutfits #menfashion #menswear #mensguides #stylish #trendy #fitness #gym #workout #streetstyle #ootd",
  },
  {
    id: 2,
    name: "All time For Men",
    size: "XXL",
    color: "Black",
    price: 3800,
    image: "https://raw.githubusercontent.com/kevinbhingaradiya/LetsUpgrade-Javascript/master/Day%207-8/img/02.jpg",
    description: "All time Body fitting T-Shirt",
  },

  {
    id: 3,

    name: "Cute Summer Workout Outfits for women",
    size: "L",
    color: "Black",
    price: 5500,
    image:
      "https://raw.githubusercontent.com/kevinbhingaradiya/LetsUpgrade-Javascript/master/Day%207-8/img/002.jpg",
    description: " classic summer workout outfit. With the basic colors in the outfit, it’s a nice way to not draw attention to yourself and keep cool.",
  },

  {
    id: 4,
    name: " Sets Yoga Outfits for Women",
    size: "L",
    color: "Black",
    price: 3500,
    image: "https://raw.githubusercontent.com/kevinbhingaradiya/LetsUpgrade-Javascript/master/Day%207-8/img/004.jpg",
    description: "Seamless Gym Clothing Women Yoga Set Fitness Workout Sets Yoga Outfits for Women Legging Women's Sportswear Suit",
  },

  {
    id: 5,
    name: "Best Stylish and Workout Outfits",
    size: "XL",
    color: "black",
    price: 2600,
    image:
      "https://raw.githubusercontent.com/kevinbhingaradiya/LetsUpgrade-Javascript/master/Day%207-8/img/03.jpg",
    description: "Best Stylish All Season Gym and Workout Outfits",
  },

  {
    id: 6,
    name: "Gym Outfits Sleeveless Hood Tshirt for Men",
    size: "M",
    color: "black",
    price: 6550,
    image:
      "https://raw.githubusercontent.com/kevinbhingaradiya/LetsUpgrade-Javascript/master/Day%207-8/img/04.jpg",
    description: "Gym Outfits Items Cotton Materials Sleeveless Hood Tshirt for Men",
  },
  {
    id: 7,
    name: "Stylish Workout Outfits for Women",
    size: "S",
    color: "Pink",
    price: 1500,
    image:
      "https://raw.githubusercontent.com/kevinbhingaradiya/LetsUpgrade-Javascript/master/Day%207-8/img/006.jpg",
    description: "Stylish Summer Workout Outfits for Women",
  },
  {
    id: 8,
    name: "Workout Set | Crop Top & Scrunch Bum Bike Shorts",
    size: "L",
    color: "Black",
    price: 7500,
    image:
      "https://raw.githubusercontent.com/kevinbhingaradiya/LetsUpgrade-Javascript/master/Day%207-8/img/008.jpg",
    description: "Stylish Crop Top & Scrunch Bum Bike Shorts Workout Outfits for Women",
  }
  
];
 let cart=[];
 let count=0;
function displaymerch(merchd, type="main", place="card") {
    console.log(merchd);
    let strmerch = "";
    let arrmerch="";
    merchd.forEach(function (ele, index) {
        if(type=="main"){
            strmerch = ` <div class="productcardbg">
        <div class="image">
          <img src="${ele.image}" width="100%" />
        </div>
        <div>
        <h3 style="font-family : Geneva, Verdana, sans-serif; padding-left : 10px">${ele.name}</h3>
        <p>Size : ${ele.size}</p>
        <p>Color : ${ele.color}</p>
        <p>price : ${ele.price}</p>
        <h5 style="font-family : Geneva, Verdana, sans-serif; padding-left : 10px ;padding-top : 10px">${ele.description}</h5>
        <p style="padding-top: 5px">
          <button class="buttonbg" onclick="addToCart(${ele.id})">Add to Cart</button>
        </p>
      </div>
      </div>`;
      arrmerch+=strmerch;
        }
        
    if(type=="cartd"){
                strmerch = ` <div class="productcardbg">
        <div class="image">
          <img src="/images/${ele.image}" width="100%" />
        </div>
        <div>
        <h3 style="font-family : Geneva, Verdana, sans-serif; padding-left : 10px">${ele.name}</h3>
        <p>Size : ${ele.size}</p>
        <p>Color : ${ele.color}</p>
        <p>price : ${ele.price}</p>
        <h5 style="font-family : Geneva, Verdana, sans-serif; padding-left : 10px ;padding-top : 10px">${ele.description}</h5>
        <p style="padding-top: 5px">
          <button class="buttonbg" onclick="deletemerch(${ele.id})">Delete item</button>
        </p>
      </div>
      </div>`;
            
        arrmerch += strmerch;    

        }
    });
    
document.getElementById(place).innerHTML = arrmerch;
    
}


function getProductByID(mercha, id) {
  return mercha.find(function (ele) {
    return ele.id == id;
  });
}
let flag=false;
function addToCart(id) {
flag=false;
  let item = getProductByID(merch, id);

  cart.forEach(function(element){
      if(element.id==item.id){
          flag=true;
          
      }
      

  })
  if (flag) {
      alert("Manners Maketh Man!\ndont add the same product twice");
    return 0;
  }
  cart.push(item);
  count+=1;
  document.getElementById("numb").innerText=count;
  let type="cartd";
  let place="cartcard";
  displaymerch(cart, type, place);

}


function search(){
    console.log("calling");
}
function deletemerch(id){
  let item = getProductByID(merch, id);
  let index = cart.findIndex(function (item1) {
    return item1.id == id;
  });
  cart.splice(index, 1);
  count-=1;
   document.getElementById("numb").innerText = count;
   let type = "cartd";
   let place = "cartcard";
   displaymerch(cart, type, place);
}

function filter(){
    let minv=document.getElementById("minp").value;
    let maxv = document.getElementById("maxp").value;
    let items= merch.filter(function(itemsl){
        return itemsl.price>=minv && itemsl.price<=maxv;
    })
     displaymerch(items);
    document.getElementById("minp").value="";
      document.getElementById("maxp").value="";
}

function search(){
    let str= document.getElementById("serstr").value;
    console.log(str);
    let items = merch.filter(function(ele) {
      return ele.name.indexOf(str)!=-1;
    });
    displaymerch(items);


}
displaymerch(merch);
const input_containerEle = document.querySelector(".input_container");
const addbtnEle = document.querySelector(".add");
const formEle = document.querySelector(".form");
const wishlist_containerEle = document.querySelector(".wishlist_container"); 

let wishListValue = "";
 let wishList = [];
 let filteredList = [];

input_containerEle.addEventListener("keyup", (event)=>{
     wishListValue = event.target.value ;
});

formEle.addEventListener('submit', (event)=>{
    event.preventDefault();
    wishList.push(wishListValue);
    input_containerEle.value = "";
    

    displayWishList(wishList);

   

});


const displayWishList = (wishList)=>{
    wishlist_containerEle.innerText = "";
    for(let wish of wishList){
        let wishContainer = document.createElement("div");

        let checkboxEle = document.createElement("input");
        checkboxEle.setAttribute("type","checkbox");
        wishContainer.appendChild(checkboxEle);


        let paraEle = document.createElement("span");
        paraEle.innerText = wish;
        wishContainer.appendChild(paraEle);
        
        let deleteBtn = document.createElement("button");
        deleteBtn.innerHTML= "delete";
        deleteBtn.setAttribute("name", wish );
        wishContainer.appendChild(deleteBtn);

        wishlist_containerEle.appendChild(wishContainer);
    }
};

    
   wishlist_containerEle.addEventListener("click", (event)=>{
    console.log(event.target);
    console.log(event.target.name);

    wishList = wishList.filter((member)=>{
        if(member != event.target.name){
            return member;
        }
    });
    displayWishList(wishList);
   }) 




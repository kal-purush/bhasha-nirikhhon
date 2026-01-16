const accessKey= "zb21mDQlwXC1-pKb5Zs6YIoWMAu_y_3QE_RR0Xo59bY";

const formEl = document.querySelector("form");
const inputEl = document.getElementById("search-input");
const searchResult = document.querySelector(".search-results");
const showMore= document.querySelector("#show-more-button");

let inputdata="";
let page = 1;

async function searchImages(){
    inputdata=inputEl.value;//whatever user will type we will create url to fetch that

    const urls= `https://api.unsplash.com/search/photos?page=${page}&query=${inputdata}&client_id=${accessKey}`;

    const response= await fetch(urls);//fetch data
    const data= await response.json();//convert into json type

    const results= data.results;//store data in results

    if(page===1){
        searchResult.innerHTML="";//if we are on 1st page no need to show anything like show more
    }

    //now we have lot of images in results so show them properly and one by one we use map variable
    results.map((result)=>{//now push all data into search-result s we will create it like it
        const imageWrapper= document.createElement("div");
        imageWrapper.classList.add("search-result")
        //now inside we have img and anchor
        const image= document.createElement("img");
        image.src=result.urls.small;//as on unsplash img are various size
        image.alt=result.alt_description;//its format
        //anchor tag
        const imageLink= document.createElement("a");
        imageLink.href=result.links.html;
        imageLink.target="_blank";
        imageLink.textContent=result.alt_description;

        imageWrapper.appendChild(image);
        imageWrapper.appendChild(imageLink);
        searchResult.appendChild(imageWrapper);//inside search results there is search result and inside searhc result there us img and img link.

    })
//now we want next images page++ and show more will be visible
    page++;
    if(page>1)
    {
        showMore.style.display="block";
    }
//till here we have completed search image but we have to add eventlistner as well
};
//this if we search for image
formEl.addEventListener("submit", (event)=>{
    event.preventDefault();
    page=1;
    searchImages();
});
//now work on show more 
showMore.addEventListener("click", ()=>{
    searchImages();
});
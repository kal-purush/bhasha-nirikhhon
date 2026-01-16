function changeImg(num){
    const foodImgElement= document.getElementById("foodstall");
    
    if(num==1){
    foodImgElement.src="./images/samosa.png";
    foodImgElement.style.height="300px";
    foodImgElement.style.margin="90px 100px";
    foodImgElement.style.marginLeft="35%";
    }
    else if(num==2){
    foodImgElement.src="./images/vada-pav.webp";
    }
    else if(num==3){
    foodImgElement.src="./images/Masala-Dosa.jpg";
    }
    else if(num==4){
    foodImgElement.src="./images/pav-bhaji.webp";
    }
    else if(num==5){
    foodImgElement.src="./images/icecream.jpg";
    }
    else if(num==6){
    foodImgElement.src="./images/maggie.avif";
    }
    else if(num==7){
    foodImgElement.src="./images/momos.webp";
    }
    }
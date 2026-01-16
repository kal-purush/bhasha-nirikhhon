var index = 1;

function showImage(n) {
    var Images = document.getElementsByClassName("slideImg");

    if (n > Images.length) {
        index = 1;
    }

    if (n < 1) {
        index = Images.length;
    }

    for (i=0; i < Images.length; i++) {
        Images[i].style.display = "none";                           //remove inline style
    }

    console.log(Images[index]);
    Images[index -1].style.display = "inline-block";                //remove inline style
}


function timedSliding () {
    index++;
    showImage(index);
}


document.getElementById("leftArrow").addEventListener("click", function (){
    index--;
    showImage(index);
})

document.getElementById("rightArrow").addEventListener("click", function (){
    index++;
    showImage(index);
})


setInterval(timedSliding,3000);
var thingCon = document.querySelectorAll(".thing-container");
if(thingCon){
  thingCon.forEach(con=>con.style.background="50% 25%/cover no-repeat url('../images/"+con.dataset.image+"')");
}

var imgCon = document.querySelectorAll(".image-container");
if(imgCon){
  imgCon.forEach(con=>con.style.background="center/cover no-repeat url('../images/"+con.dataset.image+"')");
}
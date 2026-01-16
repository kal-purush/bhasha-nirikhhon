function preloadImages(array) {
  $('#loading').show();
    if (!preloadImages.list) {
        preloadImages.list = [];
    }
    var list = preloadImages.list;
    for (var i = 0; i < array.length; i++) {
        var img = new Image();
        img.onload = function() {
            var index = list.indexOf(this);
            if (index !== -1) {
                // remove image from the array once it's loaded
                // for memory consumption reasons
                list.splice(index, 1);
            }
        }
        list.push(img);
        img.src = array[i];
    }
    $('#loading').hide();
}
let product_image = "";
let imageaspectratio = $('#imageaspectratio').val();
let imagefiletype = $('#imagefiletype').val();
let user_name = $('#user_name').val();
let photonumber = 0;
let product_name = $('#product_name').val();
function setArray()
{
  photonumber = parseInt($('#photonumber').val());
  let str = $('#productimage').val();
  let ind = str.lastIndexOf('/');
  product_image = str.substring(0, ind+1);
  let arr = [];
  for(let i = 1;i<=photonumber;i++)
  {
    arr.push(product_image+"img_"+i+"."+imagefiletype);
  }
  preloadImages(arr);
}

window.onload = setArray;

var count = 1;
var lastX;
var move;
var rotateImage = false;
var rotspeed = 60;
var swipecount = 3;
var naturalheight = '';

$("#image360").on('load', ()=> {
  naturalheight = document.getElementById('image360').naturalheight;
});

//rotation rotspeed
if(photonumber==72)
{
rotspeed = 60;
swipecount = 3;
}
else if (photonumber==36)
{
rotspeed = 120;
swipecount = 6;
}
else if(photonumber==90)
{
rotspeed = 45;
swipecount = 2;
}
else
{
rotspeed = 90;
swipecount = 4;
}

//Turn images clockwise
function leftMove()
{
  if(count==photonumber)
  {
    count = 0;
  }
count++;
let source = product_image +`img_${count}.`+imagefiletype;
$('#image360').attr('src', source);
}

//turn images anti-clockwise
function rightMove()
{
  if(count==1)
  {
    count = photonumber + 1;
  }
count--;
let source = product_image +`img_${count}.`+imagefiletype;
$('#image360').attr('src', source);
}

// event handlers on clicking buttons
$('#leftButton').on('click', ()=>{
  if(rotateImage==true)
  {stopRotate();}
  leftMove()});
$('#rightButton').on('click', ()=>{
  if(rotateImage==true)
  {stopRotate()};
  rightMove()});

//rotation on dragging the image
$('img').on('dragstart', function(event) { event.preventDefault();});
var mouseDown = false;
var prevX;
$('#image360').mousedown(function(e){
  if(rotateImage==true)
  {stopRotate();}
  prevX = e.clientX;
  mouseDown = true;
}).mousemove(function(e){
  if(mouseDown) {
    if(e.clientX-prevX<-swipecount) { leftMove();}
    if(e.clientX-prevX>swipecount) { rightMove();}
    prevX = e.clientX;
  }
}).mouseup(function(){
  mouseDown = false;
}).mouseleave(function(){
  $('#image360').mouseup();
});

//rotation on swiping
$('#image360').on('touchstart', (event)=>{
  if(rotateImage==true)
  {stopRotate()};
  event.preventDefault();
  console.log('touch');
});
$('#image360').bind('touchmove', function (e){
     var currentX = e.originalEvent.touches[0].clientX;
     if(currentX-lastX<-swipecount){
        leftMove();
     }else if(currentX-lastX>swipecount){
        rightMove();
     }
     lastX = currentX;
});

//play buuton
$('#play').on('click', ()=>{
  if(rotateImage==true)
  {
    stopRotate();
  }
  else {
    rotateImage = true;
      move = setInterval(leftMove, rotspeed);
  }
});

//stop rotation
function stopRotate()
{
  rotateImage = false;
  clearInterval(move);
}


//fullscreen images
$('#fullscreen').on('click', ()=>{
    $('#close').show();
  if(navigator.userAgent.match(/(iPad)|(iPhone)|(iPod)/i))
  {
      window.open('http://www.bawbaw.co/hosting/v360lab/fullimg.php?user_name='+user_name+'&product_name='+product_name+'&photonumber='+photonumber+'&imagefiletype='+imagefiletype);
      $('#close').hide();
  }
  if (window.matchMedia("(orientation: portrait)").matches) {
     var i = document.getElementById("image-container");
     if (i.requestFullscreen) {
     	i.requestFullscreen();
     }
     else if(i.webkitRequestFullscreen){
       i.webkitRequestFullscreen();
     }
     else if (i.mozRequestFullScreen) {
     i.mozRequestFullScreen();
    } else if (i.msRequestFullscreen) {
     i.msRequestFullscreen();
    }
    if(imageaspectratio=='4:3'||imageaspectratio=='16:9'|| imageaspectratio=='3:2')
    {
      window.screen.orientation.lock('landscape').then(null, function(error) {
        exitFullscreen();
           }
  );
    }
  }
var i = document.getElementById("image-container");
if (i.requestFullscreen) {
	i.requestFullscreen();
  changeCss(imageaspectratio);
} else if (i.webkitRequestFullscreen) {
	i.webkitRequestFullscreen();
  changeCss(imageaspectratio);
} else if (i.mozRequestFullScreen) {
	i.mozRequestFullScreen();
  changeCss(imageaspectratio);
} else if (i.msRequestFullscreen) {
	i.msRequestFullscreen();
  changeCss(imageaspectratio);
}
});

//close fullscreen
document.addEventListener('fullscreenchange', ()=>{
  if(!document.webkitIsFullScreen && !document.fullscreenElement && !document.mozFullScreen && !document.msFullscreenElement)
  {
    $('#close').hide();
    $('#image360').removeClass('full11');
    $('#image360').removeClass('full43');
  }
});
document.addEventListener('webkitfullscreenchange', ()=>{
  if(!document.webkitIsFullScreen && !document.fullscreenElement && !document.mozFullScreen && !document.msFullscreenElement)
  {
    $('#close').hide();
    $('#image360').removeClass('full11');
    $('#image360').removeClass('full43');
  }
});
document.addEventListener('mozfullscreenchange', ()=>{
  if(!document.webkitIsFullScreen && !document.fullscreenElement && !document.mozFullScreen && !document.msFullscreenElement)
  {
    $('#close').hide();
    $('#image360').removeClass('full11');
    $('#image360').removeClass('full43');
  }
});
document.addEventListener('MSFullscreenchange', ()=>{
  if(!document.webkitIsFullScreen && !document.fullscreenElement && !document.mozFullScreen && !document.msFullscreenElement)
  {
    $('#close').hide();
    $('#image360').removeClass('full11');
    $('#image360').removeClass('full43');
  }
});
$('#close').on('click', ()=>{
  $('#close').hide();
  exitFullscreen();
});

function exitFullscreen() {
               if(document.exitFullscreen) {
                  document.exitFullscreen();
                  $('#image360').removeClass('full11');
                  $('#image360').removeClass('full43');
               } else if(document.mozCancelFullScreen) {
                  document.mozCancelFullScreen();
                  $('#image360').removeClass('full11');
                  $('#image360').removeClass('full43');
               } else if(document.webkitExitFullscreen) {
                  document.webkitExitFullscreen();
                  $('#image360').removeClass('full11');
                  $('#image360').removeClass('full43');
               } else if (document.msExitFullscreen) {
                  document.msExitFullscreen();
                  $('#image360').removeClass('full11');
                  $('#image360').removeClass('full43');
               }
            }
  function changeCss(ratio){
      if(ratio=='16:9')
        {
                $('#image-container').css("width", "100%");
                $('#image360').css({"width": "100%", "height": "auto"});
        }
        else {
          if(screen.height>=naturalheight)
          {
                      $('#image360').addClass('full43');
          }
          else {
              $('#image360').addClass('full11');
          }
        }
    }
var btnplay = document.getElementsByClassName('play');
var playall = document.getElementsByClassName('playall');
var musicPlayer = document.getElementsByClassName('musicPlayer');
var waves = document.getElementsByClassName('waves');
var control = document.getElementsByClassName('control');
var audio = document.getElementById('audio');
var total = document.getElementById('total');
var current = document.getElementById('current');
var seekbarico = document.getElementsByClassName('seekbarico');
var currentvolume = document.getElementById('currentvolume');
var mute = document.getElementById('mute');
var title = document.getElementById("title");
var artist = document.getElementById("artist");
var backward = document.getElementById("backward");
var forward = document.getElementById("forward");
var image = document.getElementById('image');
var imageblur = document.getElementById('imageblur');
// var ullist = document.getElementsByClassName('ullist');
var menu = document.getElementById('menu');
var list = document.getElementsByClassName('list');
var bg = document.getElementsByClassName('bg');



var duration = 0;
var totalminute = 0;
var totalsecond = 0;
audio.ondurationchange = function() {
  // alert('5');
  duration = audio.duration;
  totalminute = Math.floor(duration / 60);
  totalminute = totalminute < 10 ? '0' + totalminute : totalminute;
  totalsecond = Math.floor(duration % 60);
  totalsecond = totalsecond < 10 ? '0' + totalsecond : totalsecond;
  var totalduration = totalminute + ':' + totalsecond;
  total.innerHTML = totalduration;
}
var seekbar = document.getElementById('seekbar');
// var seekbarico = document.getElementById('seekbarico');
var seekbarmove = document.getElementById('seekbarmove');


control[0].onmouseover = function(){
  seekbarico[0].classList.add('icoshow');
}
control[0].onmouseout = function(){
  seekbarico[0].classList.remove('icoshow');
}

function progressTime() {
  var currentTime = audio.currentTime;
  var n = currentTime / duration;
  currentminute = Math.floor(currentTime / 60);
  currentminute = totalminute < 10 ? '0' + currentminute : currentminute;
  currentsecond = Math.floor(currentTime % 60);
  currentsecond = currentsecond < 10 ? '0' + currentsecond : currentsecond;
  var currentduration = currentminute + ':' + currentsecond;
  current.innerHTML = currentduration;
  seekbarico[0].style.marginLeft = Math.floor(n * seekbar.offsetWidth) + 'px';
  seekbarmove.style.width = (n * seekbar.offsetWidth) + 'px';
}

seekbarico[0].onmousedown = function(e) {
  document.onmousemove = function(e) {
    var drag = e.clientX - musicPlayer[0].offsetLeft - seekbar.offsetLeft;
    if (drag < 0) {
      seekbarico[0].style.marginLeft = 0 + 'px';
    } else if (drag > seekbar.offsetWidth - 4) {
      seekbarico[0].style.marginLeft = seekbar.offsetWidth - 4 + 'px';
    } else {
      seekbarico[0].style.marginLeft = drag + 'px';
    }
    seekbarmove.style.width = drag < 0 ? 0 + 'px' : drag + 'px';
    seekbarmove.style.width = drag > seekbar.offsetWidth ? seekbar.offsetWidth - 4 + 'px' : drag + 'px';;
    var ratio = parseInt(getComputedStyle(seekbarico[0]).marginLeft) / (seekbar.offsetWidth - 4);
    audio.currentTime = ratio * duration;
  }
  document.onmouseup = function() {
    this.onmousedown = null;
    this.onmousemove = null;
  }

}

volumeico.onmousedown = function(e) {
  document.onmousemove = function(e) {
    var drag = e.clientX - musicPlayer[0].offsetLeft - volumebar.offsetLeft;
    if ((drag + volumebar.offsetLeft) > (volumebar.offsetLeft + volumebar.offsetWidth - 4)) {
      volumeico.style.left = (volumebar.offsetLeft + volumebar.offsetWidth - 4) + 'px';
    } else if (drag < 0) {
      volumeico.style.left = volumebar.offsetLeft + 'px';
    } else {
      volumeico.style.left = drag + volumebar.offsetLeft + 'px';
    }
    volumebarmove.style.width = drag < 0 ? 0 + 'px' : drag + 'px';
    volumebarmove.style.width = drag > volumebar.offsetWidth ? volumebar.offsetWidth + 'px' : drag + 'px';
    var ratio = (parseInt(getComputedStyle(volumeico).left) - volumebar.offsetLeft) / (volumebar.offsetWidth - 4);
    audio.volume = ratio;
    currentvolume.innerHTML = Math.floor(ratio * 100) + '%';
  }
  document.onmouseup = function() {
    this.onmousedown = null;
    this.onmousemove = null;
  }

}

onmute = false;
var currentolume;
mute.onclick = function() {
  currentolume = audio.volumevalue;
  audio.volume = 0;
  volumebarmove.style.width = 0 + 'px';
  volumeico.style.left = volumebar.offsetLeft + 'px';
  onmute = true;

}


var music = ["The Cranberries - I Can't Be With You", "Goldfrapp - Fly Me Away", "Goldfrapp - Number 1", "Hurts - Wonderful Life (Freemasons Mix)", "Depeche Mode - Heaven (Freemasons Mix)", "Depeche Mode - Heaven (Freemasons Mix)"];
// 初始化播放器

var lis = [];
var index = 0;
// bg.src = "art/" + music[index] + ".jpg";
var heighlighting = false;
creatlist();
infoHandler();

var onOff = true;
var timer;


btnplay[0].onclick = function() {
  if (onOff) {
    playHandler();
  } else {
    audio.pause();
    this.classList.remove('pause');
    onOff = true;
    waves[0].classList.remove('playing');
    clearInterval(timer);
  }
}

var playmode = 0;
audio.addEventListener('ended', listLoop);

playall[0].onclick = function() {
  playmode++;
  if (playmode == 2) {
    playmode = 0;
  }
  switch (playmode) {
    case 0:
      audio.removeEventListener('ended', singleLoop);
      audio.addEventListener('ended', listLoop);
      playall[0].classList.remove('playsingle');
      break;
    case 1:
      audio.removeEventListener('ended', listLoop);
      audio.addEventListener('ended', singleLoop);
      playall[0].classList.add('playsingle');
      break;
  }
}

// audio.addEventListener('ended', listLoop);

// audio.addEventListener('ended', singleLoop);

function listLoop() {
  if (index == music.length - 2) {
    index = -1;
  }
  index++;
  infoHandler();
  playHandler();
}

function singleLoop() {
  infoHandler();
  playHandler();
}


function infoHandler() {
  audio.src = "music/" + music[index] + ".mp3";
  image.src = "art/" + music[index] + ".jpg";
  imageblur.src = "art/" + music[index] + ".jpg";
  bg.src = "art/" + music[index] + ".jpg";
  var detail = music[index].split(' - ');
  artist.innerHTML = detail[0];
  title.innerHTML = detail[1];
}

function playHandler() {
  audio.play();
  btnplay[0].classList.add('pause');
  timer = setInterval(progressTime, 50);
  waves[0].classList.add('playing');
  onOff = false;
  playingHeighlight();
}

function playingHeighlight() {
  for (i = 0; i < lis.length; i++) {
    lis[i].classList.remove('playheighlight');
  }
  lis[index].classList.add('playheighlight');
}

// <!--上一首-->
backward.onclick = function() {
  onOff = false;
  if (index == 0) {
    index = music.length - 1;
  }
  if (index == -1) {
    index = music.length - 2;
  }
  index--;
  infoHandler();
  playHandler();
}

// <!--下一首-->
forward.onclick = function() {
  if (index == music.length - 2) {
    index = -1;
  }
  index++;
  infoHandler();
  playHandler();
}


function creatlist() {
  var ul = document.createElement('ul');
  list[0].appendChild(ul);
  for (var i = 0; i < music.length - 1; i++) {
    var li = document.createElement('li');
    var details = music[i].split(' - ');
    li.index = i;
    li.artist = details[0];
    li.title = details[1];
    li.musicname = "music/" + music[i] + ".mp3";
    li.musicimg = "art/" + music[i] + ".jpg";



    var img = document.createElement('img');
    img.src = li.musicimg;
    var h3 = document.createElement('h3');
    h3.innerHTML = li.title;
    var p = document.createElement('p');
    p.innerHTML = li.artist;
    li.appendChild(img);
    li.appendChild(h3);
    li.appendChild(p);
    li.heighlighting = false;
    lis[i] = li;
    ul.appendChild(li);
    // 给li注册事件
    li.onmouseover = liMouseOver;
    li.onmouseout = liMouseOut;
    li.onclick = liClick;
  }
}

// 当鼠标经过li的时候执行
function liMouseOver() {
  // 鼠标经过高亮显示
  this.classList.add('heighlight');
}

function liMouseOut() {
  // 鼠标离开取消高亮显示
  this.classList.remove('heighlight');
}

function liClick() {
  audio.src = this.musicname;
  image.src = this.musicimg;
  artist.innerHTML = this.artist;
  title.innerHTML = this.title;
  imageblur.src = this.musicimg;
  bg.src = this.musicimg;
  // if (index == music.length - 2) {
  //   index = -1;
  // }
  index = this.index;
  playHandler();

}



// 控制播放列表显示
var isshow = false;
menu.onclick = function() {
  // creatlist();
  if (isshow) {
    list[0].classList.remove('show');
    animate(musicPlayer[0], 150);
    animate(list[0], -150);
    isshow = false;
  } else {
    animate(list[0], 150);
    animate(musicPlayer[0], -150);
    isshow = true;
    list[0].classList.add('show');

  }
}

function animate(element, target) {
  // 通过判断，保证页面上只有一个定时器在执行动画
  if (element.timerId) {
    clearInterval(element.timerId);
    timerId = null;
  }

  var start = window.getComputedStyle(element).marginLeft;
  start = parseInt(start);
  element.timerId = setInterval(function() {
    // 步进  每次移动的距离
    var step = 20;
    // 盒子当前的位置
    var current = window.getComputedStyle(element).marginLeft;
    current = parseInt(current);
    // current = parseFloat(current);
    // 当从400 到 800  执行动画
    // 当从800 到 400  不执行动画

    // 判断如果当前位置 > 目标位置 此时的step  要小于0
    if (target < 0) {
      step = -Math.abs(step);
    }

    // Math.abs(current - target)   < Math.abs(step)
    if (Math.abs(current - start - target) < Math.abs(step)) {
      // 让定时器停止
      clearInterval(element.timerId);
      // 让盒子到target的位置
      element.style.marginLeft = start + target + 'px';
      return;
    }
    // 移动盒子
    current += step;
    element.style.marginLeft = current + 'px';
  }, 20);
}
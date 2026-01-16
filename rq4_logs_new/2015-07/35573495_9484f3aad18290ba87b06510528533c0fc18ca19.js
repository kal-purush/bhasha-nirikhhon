var NOI = 5; // number of images
var srcs = ["butterfly/01.jpg", "butterfly/02.jpg", "butterfly/03.jpg", "butterfly/04.jpg", "butterfly/05.jpg"];

var AutoPlay = function(id, oImgs) {//imgs: srcs, n, h, w
    this.initialize(id, oImgs);
};
AutoPlay.prototype = {
    initialize: function(id, oImgs){
        this.srcs = oImgs.srcs;
        this.n = oImgs.n;
        this.h = oImgs.h;
        this.w = oImgs.w;
        this.container = document.getElementById(id);
        this.timer = null;
        this.autoTimer = null;
        this.imgs = [];
        this.nums = [];
        this.currI = 0;        
        this.createSlides();
        this.createButton();
        this.nums[this.currI].className = "current";
        
        var oThis = this;
        // add event listener: when a number is clicked, the image of that index will be displayed 
        for (var i = 0; i < this.n; i++) {
            this.nums[i].index = i;
            this.nums[i].onmouseover = function() {
                clearInterval(oThis.autoTimer);
                oThis.currI = this.index;
                oThis.showCurrent();
            }
        }
        // move mouse to the image, the auto-play will stop
        this.container.onmouseover = function() {
            clearInterval(oThis.autoTimer);
        };
        // resume the auto-play
        this.container.onmouseout = function() {
            oThis.autoTimer = setInterval(function() {
                oThis.next();
            }, 3000);
        };
        this.autoMove();
    },
    createSlides: function() {
        var slides = document.createElement("ul");
        for (var i = 0; i < this.n; i++) {            
            var imgLi = document.createElement("li");
            var img = document.createElement("img");
            this.imgs.push(img);
            img.src = this.srcs[i];
            img.style.width = this.w + "px";
            img.style.height = this.h + "px";
            img.style.top = i * this.h + "px";
            imgLi.appendChild(img);
            slides.appendChild(imgLi);            
        }
        this.container.appendChild(slides);
        slides.className = "slides";
    },
    createButton: function() {
        var numbers = document.createElement("ul");
        for (var i = 0; i < this.n; i++) {
            var numberLi = document.createElement("li");
            numberLi.innerHTML = i + 1;
            this.nums.push(numberLi);
            numbers.appendChild(numberLi);
        }
        this.container.appendChild(numbers);
        numbers.className = "numbers";
    },
    
    autoMove: function() {
        var oThis = this;
        this.autoTimer = setInterval(function() {
            oThis.next();            
        }, 3000);
    },
    showCurrent: function() {
        for (var i = 0; i < this.n; i++) {
            this.nums[i].className = "";
        }
        this.nums[this.currI].className = "current";
        this.moveTo(this.imgs[this.currI], 0);        
    },
    next: function() {
        this.currI++;
        this.currI == this.n && (this.currI = 0);
        this.showCurrent();        
    },
    moveTo: function(oImg, iTarget) {
        var oThis = this;
        clearInterval(oThis.timer);
        oThis.timer = setInterval(function() {
            var speed = (iTarget - oImg.offsetTop) / oThis.n;
            speed = speed > 0? Math.ceil(speed) : Math.floor(speed);
            if (oImg.offsetTop == iTarget) {
                clearInterval(oThis.timer)
            } else {                
                for (var i = 0; i < oThis.n; i++) {                    
                    var currImg = oThis.imgs[i]; 
                   /*  x++;
                    if (x <= 250 && x > 170  && (currImg.index == 0 || currImg.index == 4)) {
                        console.log("s: " + speed);
                        console.log("i: " +currImg.index);
                        console.log(currImg.elem.offsetTop);        
                    }     */                                  
                    if ( currImg.offsetTop <= -2 * oThis.h) {
                        if (i == 0) {
                            currImg.style.top = oThis.imgs[oThis.n-1].offsetTop + oThis.h + speed + "px";
                        } else {
                            currImg.style.top = oThis.imgs[i-1].offsetTop + oThis.h + "px";
                        }  
                    } 
                    else {
                        currImg.style.top = currImg.offsetTop + speed + "px";
                    }
                    currImg = currImg.next;
                }
            }   
        }, 30);
    }
}
window.onload = function() {
    var a = new AutoPlay("container", {srcs: srcs, n: NOI, w: 490, h: 170});
    
} 
/**
 * Created by Administrator on 2016/9/21 0021.
 */
(function () {
    var banner=document.querySelector(".banner");
    var ul=document.getElementsByClassName("banner_img")[0];
    var btns=document.getElementsByClassName("banner_btns")[0].getElementsByTagName("li");
    var change=document.getElementsByTagName("p");
    var page=0;
    var time;
    var time1;
    function bannerM(banner,ul,btns,change,changeTime,bWidth) {
        move()
        time=setInterval(move,changeTime);
        function move() {
            ul.style.transition = "all 1s linear";
            ul.style.left = page * (-bWidth) + "px";
            page++;

            if (page > btns.length) {
                ul.style.transition = "all 1s linear";
                ul.style.left = "0";
                page = 0;
                time1 = setTimeout(function () {
                    ul.style.transition = "all 1s linear";
                }, 10)
            }

            for (var i = 0; i < btns.length; i++) {
                btns[i].index = i;
                btns[i].onmouseover = function () {
                    page = this.index;
                    ul.style.left = page * (-bWidth) + "px";
                    this.style.backgroundColor = "rgba(" + 255 + "," + 0 + "," + 200 + "," + 0.5 + ")";
                };
                btns[i].onmouseout = function () {
                    index = this.index;
                    ul.style.transition = "all 1s linear";
                    this.style.backgroundColor = "#fff";
                };

            }

            banner.onmouseover = function () {
                clearInterval(time);
                clearInterval(time1);
                change[0].style.display = "block";
                change[1].style.display = "block";
            };
            banner.onmouseout = function () {
                time = setInterval(move, changeTime);
                change[0].style.display = "none";
                change[1].style.display = "none";
            };


            change[0].onclick = function () {
                clearInterval(time);
                clearInterval(time1);
                ul.style.left = (page - 1) * (-bWidth) + "px";
                page--;
                if (page < 0) {
                    ul.style.transition = "all 1s linear";
                    ul.style.left = (btns.length - 1) * (-bWidth) + "px";
                    page = btns.length - 1;
                }
            };
            change[1].onclick = function () {
                clearInterval(time);
                clearInterval(time1);

                ul.style.left = (page) * (-bWidth) + "px";
                page++;
                if (page > btns.length) {
                    ul.style.transition = "all 1s linear";
                    ul.style.left = "0";
                    page = 0;
                }
            };

        }

    }
    bannerM(banner,ul,btns,change,2000,1024);
})();
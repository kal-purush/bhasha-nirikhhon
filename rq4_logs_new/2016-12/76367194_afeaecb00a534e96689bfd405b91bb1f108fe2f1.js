

/*
$(function() {
    var i = 0;
    $.each(document.getElementsByClassName("responsive-img"), function () {
        var cvs = document.createElement('canvas');
            img = document.getElementsByClassName("responsive-img")[i];
        cvs.width = img.width; cvs.height = img.height;
        var ctx = cvs.getContext("2d");
        ctx.drawImage(img, 0, 0, cvs.width, cvs.height);
        var idt = ctx.getImageData(0, 0, cvs.width, cvs.height);

        red = idt.data[0];
        green = idt.data[1];
        blue = idt.data[2];

        //alert(red + " " + green + " " + blue);


        //$('.thumbnail').get(i).css("background-color", "rgba(" + red + "," + green + "," + blue + ",255)");
        i++;
    });
});

$(window).load(function() {
    var i = 0;
    $.each(document.getElementsByClassName("responsive-img"), function () {
        var cvs = document.createElement('canvas');
        img = document.getElementsByClassName("responsive-img")[i];
        cvs.width = img.width; cvs.height = img.height;
        var ctx = cvs.getContext("2d");
        ctx.drawImage(img, 0, 0, cvs.width, cvs.height);
        var idt = ctx.getImageData(0, 0, cvs.width, cvs.height);

        red = idt.data[0];
        green = idt.data[1];
        blue = idt.data[2];

        //alert(red + " " + green + " " + blue);


        //$('.thumbnail').get(i).css("background-color", "rgba(" + red + "," + green + "," + blue + ",255)");
        i++;
    });
});*/

$(window).scroll(function (){
    var scroll = $(document).scrollTop();
    if(scroll >= 100){
        $(".nav").removeClass("big");
        $(".nav-hilfe").removeClass("nav-content");
        $(".content").addClass("small");
    }
    else if(scroll < 100){
        $(".nav").addClass("big");
        $(".nav-hilfe").addClass("nav-content");
        $(".content").removeClass("small");
    }
});

$(".responsive-img").click(function(){
    var imgName = $(this).attr("src");

    var imgClass = "img-big-big";

    var html = '<div class="img-big"><img class="'+imgClass+'" src="'+imgName+'"></div>';

    var winwidthold = 0;
    var winheightold = 0;

    var tempElement = document.createElement('div');
    tempElement.innerHTML = html;
    document.getElementsByTagName("body")[0].appendChild(tempElement.firstChild);

    var winwidth = $(".img-big").width();
    var winheight = $(".img-big").height();

    var width = $(".img-big-big").width();
    var height = $(".img-big-big").height();

    var ges;

    if(width >= height){
        if(width >= winwidth && winheight-50 <= (height*(winwidth-50)/width)){
            $(".img-big-big").css("height", (winheight - 50)+"px");
            $(".img-big-big").css("width", "auto");
            $(".img-big-big").css("margin-top", 25);
        }
        else {
            $(".img-big-big").css("width", (winwidth - 50)+"px");
            $(".img-big-big").css("height", "auto");
            $(".img-big-big").css("margin-top", (winheight-(height*(winwidth-50)/width))/2);
        }
    }
    else if(height > width){
        if(height >= winheight && winwidth-50 <= (width*(winheight-50)/height)){
            $(".img-big-big").css("width", (winwidth - 50)+"px");
            $(".img-big-big").css("height", "auto");
            $(".img-big-big").css("margin-top", (winheight-(height*(winwidth-50)/width))/2);
        }
        else{
            $(".img-big-big").css("height", (winheight - 50)+"px");
            $(".img-big-big").css("width", "auto");
            $(".img-big-big").css("margin-top", 25);
        }
    }

    winwidthold = winwidth;
    winheightold = winheight;

    $(window).resize(function() {
        winwidth = $(".img-big").width();
        winheight = $(".img-big").height();

        width = $(".img-big-big").width();
        height = $(".img-big-big").height();

        if (winwidth < winwidthold) {
            if(width > winwidth-50){
                $(".img-big-big").css("width", (winwidth - 50)+"px");
                $(".img-big-big").css("height", "auto");
                $(".img-big-big").css("margin-top", (winheight-height)/2);
            }
        }
        else if (winwidth > winwidthold) {
            if(height >= winheight-50){
                $(".img-big-big").css("height", (winheight - 50)+"px");
                $(".img-big-big").css("width", "auto");
                $(".img-big-big").css("margin-top", (winheight-height)/2);
            }
            else{
                $(".img-big-big").css("width", (winwidth - 50)+"px");
                $(".img-big-big").css("height", "auto");
                $(".img-big-big").css("margin-top", (winheight-height)/2);
            }
        }
        else if (winheight < winheightold){
            if(height < winheight-50){
                $(".img-big-big").css("width", (winwidth - 50)+"px");
                $(".img-big-big").css("height", "auto");
                $(".img-big-big").css("margin-top", (winheight-height)/2);
            }
            if(height >= winheight-50){
                $(".img-big-big").css("height", (winheight - 50)+"px");
                $(".img-big-big").css("width", "auto");
                $(".img-big-big").css("margin-top", (winheight-height)/2);
            }
        }
        else if (winheight > winheightold){
            if(height <= winheight-50 && width >= winwidth-50){
                //alert('hallo');
                $(".img-big-big").css("width", (winwidth - 50)+"px");
                $(".img-big-big").css("height", "auto");
                $(".img-big-big").css("margin-top", (winheight-height)/2);
            }
            else if(height <= winheight-50 && width < winwidth-50){
                $(".img-big-big").css("height", (winheight - 50)+"px");
                $(".img-big-big").css("width", "auto");
                $(".img-big-big").css("margin-top", (winheight-height)/2);
            }
        }


        winwidthold = winwidth;
        winheightold = winheight;
    });

    $(".img-big").click(function(){
        $(".img-big").remove();
    });
});
/*


 */


//Speed of the show/hide animations
var fadein=700;
var fadeout=0;
var bgcolor;


//Set the color for the different library colors
$( ".library-red").hover(function() {
    bgcolor = "#ff3535";
});

$( ".library" ).hover(function() {
    bgcolor = "#5a5a5a";
});
$( ".library-black" ).hover(function() {
    bgcolor = "#393434";
});
$( ".library-blue" ).hover(function() {
    bgcolor = "#2f6be3";
});
$( ".library-purple" ).hover(function() {
    bgcolor = "#c02fe5";
});
$( ".library-green" ).hover(function() {
    bgcolor = "#3dbd5a";
});



//Sets the styling for inactive elements
function outColor(element){
    $( element ).css( "background-color", "transparent" );
    $( element ).css( "color", "#E7E7E7" );

}
//Sets the styling for active elements
function inColor(element){
    $( element ).css( "background-color", bgcolor );
    $( element ).css( "color", "white" );
}




//Show Lib1
$("#lib-nav1").click(function(){
    $("#lib1").fadeIn(fadein,inColor('#lib-nav1'));
    $("#lib2").fadeOut(fadeout,outColor('#lib-nav2'));
    $("#lib3").fadeOut(fadeout,outColor('#lib-nav3'));

    $("#lib-active1").show();
    $("#lib-active2").hide();
    $("#lib-active3").hide();
});

//Show Lib2
$("#lib-nav2").click(function(){
    $("#lib1").fadeOut(fadeout,outColor('#lib-nav1'));
    $("#lib2").fadeIn(fadein,inColor('#lib-nav2'));
    $("#lib3").fadeOut(fadeout,outColor('#lib-nav3'));

    $("#lib-active1").hide();
    $("#lib-active2").show();
    $("#lib-active3").hide();
});

//Show Lib3
$("#lib-nav3").click(function(){
    $("#lib1").fadeOut(fadeout,outColor('#lib-nav1'));
    $("#lib2").fadeOut(fadeout,outColor('#lib-nav2'));
    $("#lib3").fadeIn(fadein,inColor('#lib-nav3'));

    $("#lib-active1").hide();
    $("#lib-active2").hide();
    $("#lib-active3").show();
});
/*

 */


//Show/hide elements on the first visit
//of the website
window.onload=function(){
    //panel
    $("#panel1").show();
    $("#panel2").hide();
    $("#panel3").hide();

    //library
    $("#lib1").show();
    $("#lib2").hide();
    $("#lib3").hide();

    $("#lib-active1").show();
    $("#lib-active2").hide();
    $("#lib-active3").hide();
};
/*


 */

//Speed of the show/hide animations
var show=500;
var hide=800;



//Show Panel1
$("#nav1").click(function(){
    $("#panel1").show(show);
    $("#panel2").hide(hide);
    $("#panel3").hide(hide);
});

//Show Panel2
$("#nav2").click(function(){
    $("#panel1").hide(hide);
    $("#panel2").show(show);
    $("#panel3").hide(hide);
});

//Show Panel3
$("#nav3").click(function(){
    $("#panel1").hide(hide);
    $("#panel2").hide(hide);
    $("#panel3").show(show);
});
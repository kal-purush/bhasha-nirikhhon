import Swiper from 'swiper';
import './../less/test.less';
import Img from './../assets/contact_kv.png';
import Page from './../utils/page'
import contentArt from "./../art/test.art";

let data =[
    {
        img:Img
    },
    {
        img:Img
    }
];

const contentHtml = contentArt({Img:Img,data:data});


Page(contentHtml).then((val)=>{
    //console.log(val)
    initHTMl()
});
/*
setTimeout(function(){
    console.log(1)

    //document.querySelector('#App').innerHTML=contentHtml;

    Page(contentHtml).then((val)=>{
        console.log(val)
        initHTMl()
    });

},1000)*/

console.log()
initHTMl();

function initHTMl(){
    var swiper = new Swiper('.swiper-container', {
        navigation: {
            nextEl: '.swiper-button-next',
            prevEl: '.swiper-button-prev',
        },
        pagination: {
            el: '.swiper-pagination',
        },
    });
}


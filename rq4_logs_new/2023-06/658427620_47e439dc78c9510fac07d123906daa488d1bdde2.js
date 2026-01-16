const review = [
    {
      id:1,
      name:"NAMJOON",
      photo:"https://www.artnews.com/wp-content/uploads/2022/12/GettyImages-1296098783.jpg",
      about:"RM (Rap Monster): Kim Namjoon, also known as RM, is the leader of BTS. Born on September 12, 1994, RM is not only a skilled rapper but also a talented songwriter and producer. Known for his introspective and thought-provoking lyrics, he often explores deep philosophical and social themes. RM's charismatic leadership and his ability to bridge cultures through his fluent English have played a pivotal role in BTS's global success."
      
    },
    {
      id:2,
      name:"KIM SEOKJIN",
      photo:"https://image.winudf.com/v2/image1/YnRzamluLndhbGxwYXBlcnNfc2NyZWVuXzBfMTYwNjMwODI1Nl8wNjk/screen-0.webp?fakeurl=1&type=.webp",
      about:"Kim Seokjin, also known as Jin, is a prominent member of the internationally acclaimed South Korean boy band BTS. Born on December 4, 1992, Jin has captivated audiences around the world with his exceptional talent, charismatic personality, and stunning visuals.Jin's journey as an artist began when he joined Big Hit Entertainment and debuted as the oldest member of BTS in 2013. As a vocalist, he showcases his remarkable vocal range, soothing tone, and emotive delivery, leaving a lasting impact on listeners. His vocal abilities shine through in numerous BTS songs, including his solo track EPIPHANY which beautifully encapsulates his growth and self-discovery"
      
    },
    {
      id:3,
      name:"MIN YOONGI",
      photo:"https://images.indianexpress.com/2021/12/suga1.jpg",
      about:"Min Yoongi, widely known as Suga, is a prominent member of the renowned South Korean boy band BTS. Born on March 9, 1993, Suga has captivated audiences worldwide with his exceptional talent as a rapper, producer, and songwriterSuga's journey in the music industry began long before his BTS debut. He honed his skills as an underground rapper under the name Agust D, releasing mixtapes that showcased his raw and introspective lyricism. His ability to convey profound emotions through his words has resonated deeply with fans, earning him critical acclaim as a rapper."
      
    },
    {
      id:1,
      name:"JUNG HOSEOK",
      photo:"https://images.news18.com/ibnlive/uploads/2023/02/bts-jhope-16766386914x3.jpg",
      about:"J-Hope: Jung Hoseok, known as J-Hope, is a rapper, dancer, and songwriter. Born on February 18, 1994, he brings vibrant energy to BTS's performances with his exceptional dance skills and captivating stage presence. J-Hope's positive and cheerful personality shines through his music, injecting a sense of optimism and hope. He has also released his own solo music, showcasing his versatility as an artist."
      
    },
    {
      id:1,
      name:"PARK JIMIN",
      photo:"https://st1.bollywoodlife.com/wp-content/uploads/2022/08/BTS-Jimin.jpg?impolicy=Medium_Widthonly&w=1280&h=900",
      about:"Park Jimin, often referred to simply as Jimin, is a talented singer and dancer. Born on October 13, 1995, he is known for his smooth vocals and precise dance moves. Jimin's performances are marked by his captivating stage presence and emotional expressiveness. He brings a blend of power and grace to BTS's music, leaving a lasting impression on audiences worldwide."
      
    },
    {
      id:1,
      name:"KIM TAEHYUNG",
      photo:"https://st1.bollywoodlife.com/wp-content/uploads/2021/07/MicrosoftTeams-image-8.jpg",
      about:"Kim Taehyung, also known as V, is a singer, songwriter, and actor. Born on December 30, 1995, V is recognized for his deep and soulful vocals, often characterized by their unique tone and versatility. Beyond his singing talent, V has showcased his acting skills in various projects, further expanding his artistic repertoire. His enigmatic personality and captivating visuals have earned him a dedicated fanbase."
      
    },
    {
      id:1,
      name:"JOEN JUNGKOOK",
      photo:"https://w0.peakpx.com/wallpaper/342/318/HD-wallpaper-jungkook-bangtan-sonyeondan-bts-bts-be-bts-fila-bts-jungkook-bts-life-goes-on-jeon-jungkook-jk-jungkook-fila.jpg",
      about:"Jeon Jungkook, the youngest member of BTS, was born on September 1, 1997. Often referred to as the Golden Maknae, Jungkook excels in singing, dancing, and even rapping. His powerful vocals and impressive range have made him a standout member of the group. With his dedicated work ethic and natural talent, Jungkook continues to evolve as a versatile artist, leaving a lasting impact on fans worldwide."
      
    }
];
var count  =0;
const NAME = document.getElementById('name');
const PARA = document.getElementById('text');
const IMG = document.getElementById('image');

window.addEventListener('DOMContentLoaded',function(){
   createObject(count);
})

function createObject(count){
  IMG.src = review[count].photo;
  NAME.textContent = review[count].name;
  PARA.textContent = review[count].about;
}

const prv = document.querySelector(".prev");
const nxt = document.querySelector(".next");
const sup = document.querySelector(".sup");

prv.addEventListener('click',function(){
  count--;
  if(count<0) count = review.length-1;
  createObject(count);
  
})
nxt.addEventListener('click',function(){
  count++;
  if(count > review.length-1) count = 0;
  createObject(count);
  
})
sup.addEventListener('click',function(){
    var c = random();
    createObject(c);
})

function random(){
  var randomNumber = Math.floor(Math.random()*review.length);
  return randomNumber;
}

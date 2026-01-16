/* 
* @Author: Marte
* @Date:   2018-02-09 17:43:46
* @Last Modified by:   Marte
* @Last Modified time: 2018-02-09 17:46:22
*/

$(document).ready(function(){
 console.log(666);
 var idx = location.href.indexOf("?");
 var id = location.href.slice(idx+1);
 console.log(id);   
 let arr_status=[200,304];
 let xhr=new XMLHttpRequest();
 // 获取页面数据
 let goods=document.querySelector('.center_1');
 console.log(goods);
 xhr.onreadystatechange = function(){
    if(xhr.readyState === 4 && arr_status.includes(xhr.status)>=0){
        console.log("获取成功");
        console.log(xhr.responseText);
        let res= JSON.parse(xhr.responseText);
        let ul=document.createElement('ul');
        // let price=res[1].add_time;
         console.log(res);

         // console.log(price);
        ul.innerHTML=res.map(item=>{
            return`<a><li data-id="${item.id}" data-category="${item.category}">
            <img src="${item.img}" data-big="${item.img}">
                <h4>${item.name}:</h4></br>
                <h3>${item.price}</h3></br>
                <span>前往购物车</span>
            </li></a>`
        }).join('');
        goods.appendChild(ul);
        console.log(ul);
    }
}
xhr.open('get','../api/goods.php?id='+id,true);
xhr.send();


});
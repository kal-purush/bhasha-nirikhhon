/*---------   document ready   -----------*/
$(document).ready(function() {


//*********//
$(function() {                       
  $(".w_500_1").click(function() {  
    $(".content-block2").addClass("content-block2-active");
	$(".input").addClass("name1-active");
	$(".w_500_1").addClass("w_500_1-active");
    $(".input").removeClass("name2-active");
    $(".input").removeClass("name3-active");
    $(".w_500_2").removeClass("w_500_2-active");
    $(".w_500_3").removeClass("w_500_3-active");
  });
});
$(function() {                       
  $(".w_500_2").click(function() {  
    $(".content-block2").addClass("content-block2-active");
    $(".input").addClass("name2-active");
	$(".w_500_2").addClass("w_500_2-active");
	$(".input").removeClass("name1-active");
    $(".input").removeClass("name3-active");
    $(".w_500_1").removeClass("w_500_1-active");
    $(".w_500_3").removeClass("w_500_3-active");
  });
});
$(function() {                       
  $(".w_500_3").click(function() {  
    $(".content-block2").addClass("content-block2-active");
    $(".input").addClass("name3-active");
	$(".w_500_3").addClass("w_500_3-active");
	$(".input").removeClass("name1-active");
    $(".input").removeClass("name2-active");
    $(".w_500_1").removeClass("w_500_1-active");
    $(".w_500_2").removeClass("w_500_2-active");
  });
});

$(function() {                       
  $(".x").click(function() {  
    $(".content-block2").removeClass("content-block2-active");
  });
});



/*
let tr_list = document.querySelector('.overflow__table2').getElementsByTagName('tr');
for(let i=0; i<tr_list.length; i++){
	tr_list[i].getElementsByTagName('td')[3].className='attention';
	tr_list[i].getElementsByTagName('td')[4].className='attention';
}
*/


//*********//
let json = '[{"add":"add1", "age":"age1", "family":"family1", "job":"job1", "edu":"edu1"}, {"add":"add2", "age":"age2", "family":"family2", "job":"job2", "edu":"edu2"}, {"add":"add3", "age":"age3", "family":"family3", "job":"job3", "edu":"edu3"}]';
let jsonParse = JSON.parse(json);
for (let key in jsonParse) {
  let p1 = document.createElement('p');
  let p2 = document.createElement('p');
  let p3 = document.createElement('p');
  let p4 = document.createElement('p');
  let p5 = document.createElement('p');
  let div = document.createElement('div');
  div.className = "div" + jsonParse[key].add;
  p1.innerHTML = "Добавлен: " + jsonParse[key].add;
  p2.innerHTML = "Возраст: " + jsonParse[key].age;
  p3.innerHTML = "Семейное положение: " + jsonParse[key].family;
  p4.innerHTML = "Работает: " + jsonParse[key].job;
  p5.innerHTML = "Образование: " + jsonParse[key].edu;
  div.appendChild(p1);
  div.appendChild(p2);
  div.appendChild(p3);
  div.appendChild(p4);
  div.appendChild(p5);
  $('.input').append(div);
};



//*************    end document ready    *********//
});
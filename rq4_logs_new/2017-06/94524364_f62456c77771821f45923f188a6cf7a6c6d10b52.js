$("document").ready(function(){

$(".name_text").keyup(function(e){
	var code = e.which;
	if (code == 13){
		e.preventDefault();
		var con = confirm(e.target.value+ ": welcome to the world of FITNESS and LUCK");
		if (con){
			window.location.replace("file:///C:/Users/MIS/Desktop/excercise%20planner/BMI.html")
		}
	}
})

$(".bmi_right").hide();


$(".age").keyup(function(e){
	var num = parseInt(e.target.value);
	if((!Number.isInteger(num)&&!(e.which ==13)&&!(e.which ==16)&&!(e.which ==9)&&!(e.which ==8))||(num > 118)){
		alert("please enter your real age");
		$(this).css('background-color','pink');
	}
	else{
		$(this).css('background-color','white');
	}
})


$(".height").keyup(function(e){
	var num = parseInt(e.target.value);
	if((!Number.isInteger(num)&&!(e.which ==13)&&!(e.which ==16)&&!(e.which ==9)&&!(e.which ==8))||(num > 3)){
		alert("please enter in meter");
		$(this).css('background-color','pink');
	}
	else{
		$(this).css('background-color','white');
	}
})

$(".weight").keyup(function(e){
	var num = parseInt(e.target.value);
	if((!Number.isInteger(num)&&!(e.which ==13)&&!(e.which ==16)&&!(e.which ==9)&&!(e.which ==8))||(num > 420)){
		alert("please enter in kilogram");
		$(this).css('background-color','pink');
	}
	else{
		$(this).css('background-color','white');
	}
})

$(".bmi_middle").click(function(){
	var res = ($("[name='u_weight']").val()*1)/ (($("[name='u_height']").val()*1)*($("[name='u_height']").val()*1))
	var result = res.toFixed(2)
	$(".bmi_right").text(result+" %");
	if (result > 30.00){
		$(".bmi_right").append("<p> Obese </p>");
	} else if (result > 18.50){
		$(".bmi_right").append("<p> Normal </p>");
	} else if (result > 16.00){
		$(".bmi_right").append("<p> Moderate Thinness </p>");
	} else if(result < 16.00){
		$(".bmi_right").append("<p> Severe Thinness </p>");
	}
	  else {
	  	$(".bmi_right").append("<p> No data entered </p>");
	  }
	$(".bmi_right").fadeIn("3000");
})


var text_1 = "";
var timeC = new Date().getSeconds();
switch(timeC%9){
	case 0: text_1 = "<br><p> RUNNING</p><p>30 MIN </p>"; break;
	case 1: text_1 = "<br><p> CYCLING</p><p>30 MIN </p>"; break;
	case 2: text_1 = "<br><p> WALKING</p><p>1.5 HOURS </p>"; break;
	case 3: text_1 = "<br><p> SWIMMING</p><p>30 MIN </p>"; break;
	case 4: text_1 = "<br><p> ROWING</p><p>30 MIN </p>"; break;
	case 5: text_1 = "<br><p> JUMPING ROPE</p><p> 1 HOUR </p>"; break;
	case 6: text_1 = "<br><p> STEPPERS</p><p>1 HOUR </p>"; break;
	case 7: text_1 = "<br><p> CROSS TRAINING</p><p>30 MIN </p>"; break;
	case 8: text_1 = "<br><p> RUNNING</p><p>30 MIN </p>"; break;
}

var text_2 = "";
var timeC = new Date().getSeconds();
switch(timeC%8){
	case 0: text_2 = "<br><p> SQUAT</p><p>15 per Group</p><p>3 GROUPS</p>"; break;
	case 1: text_2 = "<br><p> WALL SIT</p><p>15 per Group</p><p>3 GROUPS</p>"; break;
	case 2: text_2 = "<br><p> LEG CURL</p><p>15 per Group</p><p>3 GROUPS</p>"; break;
	case 3: text_2 = "<br><p> BACK EXTENSION</p><p>15 per Group</p><p>3 GROUPS</p>"; break;
	case 4: text_2 = "<br><p> DEADLIFT</p><p>15 per Group</p><p>3 GROUPS</p>"; break;
	case 5: text_2 = "<br><p> PECTORALS</p><p>15 per Group</p><p>3 GROUPS</p>"; break;
	case 6: text_2 = "<br><p> LEG EXTENSION</p><p>15 per Group</p><p>3 GROUPS</p>"; break;
	case 7: text_2 = "<br><p> TRICEPS</p><p>15 per Group</p><p>3 GROUPS</p>"; break;
	case 8: text_2 = "<br><p> SNATCH</p><p>15 per Group</p><p>3 GROUPS</p>"; break;
}

var text_3 = "";
var timeC = new Date().getSeconds();
switch(timeC%7){
	case 0: text_3 = "<br><p> SQUAT</p><p>15 per Group</p><p>3 GROUPS</p>"; break;
	case 1: text_3 = "<br><p> WALL SIT</p><p>15 per Group</p><p>3 GROUPS</p>"; break;
	case 2: text_3 = "<br><p> LEG CURL</p><p>15 per Group</p><p>3 GROUPS</p>"; break;
	case 3: text_3 = "<br><p> BACK EXTENSION</p><p>15 per Group</p><p>3 GROUPS</p>"; break;
	case 4: text_3 = "<br><p> DEADLIFT</p><p>15 per Group</p><p>3 GROUPS</p>"; break;
	case 5: text_3 = "<br><p> PECTORALS</p><p>15 per Group</p><p>3 GROUPS</p>"; break;
	case 6: text_3 = "<br><p> LEG EXTENSION</p><p>15 per Group</p><p>3 GROUPS</p>"; break;
	case 7: text_3 = "<br><p> TRICEPS</p><p>15 per Group</p><p>3 GROUPS</p>"; break;
	case 8: text_3 = "<br><p> SNATCH</p><p>15 per Group</p><p>3 GROUPS</p>"; break;
}

$(".box").click(function(){
	if ($(".box").hasClass("selected")){
		alert("You had already used your luck on this part. Please choose from the next catagory")
	}else{
	$(this).addClass("selected");
	$(this).append(text_1);
	$(this).css({'font-size': '18px', 'text-align': 'center', 'font-weight':'bold',})	
		}
})


$(".box2").click(function(){
	if ($(".box2").hasClass("selected2")){
		alert("You had already used your luck on this part. Please choose from the next catagory")
	}else{
	$(this).addClass("selected2");
	$(this).append(text_2);
	$(this).css({'font-size': '15px', 'text-align': 'center', 'font-weight':'bold'})	
		}
})


$(".box3").click(function(){
	if ($(".box3").hasClass("selected3")){
		alert("You had already made all your choices. Now, GO and START")
	}else{
	$(this).addClass("selected3");
	$(this).append(text_3);
	$(this).css({'font-size': '15px', 'text-align': 'center', 'font-weight':'bold'})	
		}
})

/*$(function(){
	$(".tip_images img:gt(0)").hide();
	setInterval(function() {
	 	$('.tip_images :first-child').fadeOut();
	 	//$('orange img').attr('src', 'images/dot2.png');
	    .next().fadeIn();
	    //$('green2 img').attr('src', 'images/dot1.png');
	    end().appendTo('.tip_images');
	}, 3000);
})
*/
/*$(function(){
    $('.tip_images img:gt(0)').hide();
    setInterval(function(){
     $('.tip_images img:gt(0)').fadeOut(1000);
     $('.tip_images img:gt(1)').fadeIn(1000);
     $('.tip_images img:gt(1)').fadeOut(1000);
     $('.tip_images img:gt(2)').fadeIn(1000);}, 
      6000);
});
*/


$('.tip_images img:gt(0)').hide();
$(function(){
    
    setInterval(function(){
      $('.tip_images :first-child').fadeOut()
         .next('img').fadeIn()
         .end().appendTo('.tip_images');}, 
      3000);
});

var intervalFunctions = [B, C, A];
var intervalIndex = 0;
$(function(){
    setInterval(function(){
     intervalFunctions[intervalIndex++ % intervalFunctions.length]();
  }, 
      3000);
});

function A(){
      $('.rbar_number img').attr('src', 'images/dot2.png')
      $('.rbar_number :first-child').attr('src', 'images/dot1.png')
  }

function B(){
      $('.rbar_number img').attr('src', 'images/dot2.png')
      $('.rbar_number img:nth-child(2)').attr('src', 'images/dot1.png')
  }

function C(){
    $('.rbar_number img').attr('src', 'images/dot2.png')
    $('.rbar_number img:nth-child(3)').attr('src', 'images/dot1.png')
  }




var dislikeE = "run";
var likeE = "run";
var aimE = "run";
$("#dlike_E").change(function(){
	$(".interest_right").hide();
	dislikeE = $(this).val();
})
$("#like_E").change(function(){
	$(".interest_right").hide();
	likeE = $(this).val();
})
$("#aim_E").change(function(){
	$(".interest_right").hide();
	aimE = $(this).val();
})
$(".interest_right").hide();

$(".interest_middle").click(function(){
	if (dislikeE == likeE){
	alert("You can not like and dislike an excercise at the same time");
	}
	switch(aimE){
		case "swim": $("#cardio_ex").html("Swimming "); break;
		case "bike": $("#cardio_ex").html("Biking ");break;
		case "run": $("#cardio_ex").html("Running ");break;
		default : 	
			switch(likeE){
			case "swim": $("#cardio_ex").html("Swimming "); break;
			case "bike": $("#cardio_ex").html("Biking ");break;
			case "run": $("#cardio_ex").html("Running ");break;
			default : break; }
		}
	if (dislikeE != aimE){
	switch(dislikeE){
		case "swim": $("#cardio_ex").html("Running "); break;
		case "bike": $("#cardio_ex").html("Running ");break;
		case "run": $("#cardio_ex").html("Biking ");break;
		case "weight": $("#strength_ex").html("Push-up");
					   $("#strength_t").html("50");
					   $("#strength2_ex").html("Plank");
					   $("#strength2_t").html("10 MIN");
		default : break;
	}
}

	$(".interest_right").fadeIn(2000);
	
})

$(".agree_status").click(function(){
	alert("Automatically Login is not recommend if this is not your computer");
})






})
// VERSION 1.01

var winWidth,winHeight,gameOb,resizeTimer;

var postsWidth = 250;
var postsHeight = 500;

var postsHFactor = postsWidth/postsHeight;
var postsWFactor = postsHeight/postsWidth;

var myElement,mc;

var initLoad = 0;

//var mobile = false;

var skyRatio = 394/2000;

var myLoader = html5Preloader();

var playerTicker = 0;

var players = [

	{
		name:'Shaun Johnson',
		src:'player_01.jpg'
	},

	{
		name:'Manu Vatuvei',
		src:'player_02.jpg'
	},

	{
		name:'Sam Tomkins',
		src:'player_03.jpg'
	},

	{
		name:'Konrad Hurrell',
		src:'player_04.jpg'
	},

	{
		name:'Stacey Jones',
		src:'player_05.jpg'
	},

	{
		name:'Ruben Wiki',
		src:'player_06.jpg'
	}

];

var curTut = 1;
var newUser = true;
var tut_top = 40;
var tut_bottom = 100;

var num_players = players.length;

var selectedPlayer = {

};

var user = {};

var isIE = false;

$(document).ready(function(){

	if($('html').hasClass('ie')){

		isIE = false;

	};

	$.logThis("ie :> "+isIE);

	if(!mobile){

		$('#mobileModal').foundation('reveal', 'open');

	}

	//$.logThis("num_players :> "+num_players);

	myLoader.addFiles('img/stadium_01_front.png','img/ball-logo.png','img/voda.png','img/stadium_01_right.png','img/stadium_01_left.png','img/warriors-logo.png','img/game_logo.png','img/player_01.jpg','img/player_02.jpg','img/player_03.jpg','img/player_04.jpg','img/player_05.jpg','img/player_06.jpg','img/app_bg.jpg','img/grass_01_left.jpg','img/grass_01_right.jpg','img/grass_01_front.jpg','img/sky_bg_grad.jpg'); 
	
	myLoader.on('finish', function(){
		
		$('#loading').fadeOut(500,function(){
			
			//$('#stage1').fadeIn(1000,function(){

				loadNextStage("#loading","#stage1");
		
				gameLoaded();
				
			//});
		
		});
	
	});

	placeholderFix();

	

});

function loadNextStage(prev,next){

	$.logThis(prev +" / "+next);

	$(prev).removeClass('current').hide();
	$(next).addClass("current").show();

	positionInitScreen();

}

function gameLoaded(){

	myElement = document.getElementById('action_layer');
	mc = new Hammer(myElement);

	winWidth = $("#container").innerWidth();
	winHeight = $("#container").innerHeight();

	resizeListeners();

	$(".current .init_info").css('margin-top', ((winHeight - $(".current .init_info").outerHeight())/2)*0.8);

	$('.enter_btn').on('click',function(e){

		e.preventDefault();

		$.logThis("enter");

		loadNextStage("#stage1","#stage2");

	});

	$('#login_btn').on('click',function(e){

		e.preventDefault();

		loadNextStage("#stage2","#stage2b");
		
		

	});

	$('#signup_btn').on('click',function(e){

		e.preventDefault();

		loadNextStage("#stage2","#stage2a");

	});

	$('.play_now').on('click',function(e){

		e.preventDefault();

		loadNextStage("#stage3","#loading");

		selectedPlayer.name = players[playerTicker].name;

		selectedPlayer.src = players[playerTicker].src;

		ga('send', 'event', 'game stats', 'player '+selectedPlayer.name, 1);


		if(mobile){
		
			$("#game").fullScreen(true);

			setTimeout(function(){

	    		logPlay();

	    	},1500);

		}else{

			logPlay();

		}
		

	});

	$('#game').on('click', '.leaders_all',function(e){

		e.preventDefault();

		$.post(

			'includes/get_all_leaders.php',

			{
				nonce:nonce
			},

			function(data){

				if(data.status){

					$('#leaders_header').html("TOP 10 OVERALL");
					$("#leaders_text").html(data.html);
					$('#leadersModal').foundation('reveal', 'open');
					ga('send', 'event', 'game stats', 'view all leaders', 1);

				}else{

					$('#alert_text').html("<p>encounted an issue reconecting to the server please refresh your browser ERROR:06</p>");
					$('#alertModal').foundation('reveal', 'open');

				}

			},

			'json'

		);

	});

	$('#game').on('click', '.leaders_daily',function(e){

		e.preventDefault();

		$.post(

			'includes/get_daily_leaders.php',

			{
				nonce:nonce
			},

			function(data){

				if(data.status){

					$('#leaders_header').html("TOP 10 TODAY");
					$("#leaders_text").html(data.html);
					$('#leadersModal').foundation('reveal', 'open');
					 ga('send', 'event', 'game stats', 'view daily leaders', 1);

				}else{

					$('#alert_text').html("<p>encounted an issue reconecting to the server please refresh your browser ERROR:06</p>");
					$('#alertModal').foundation('reveal', 'open');

				}

			},

			'json'

		);

	});

	$('#share_fb').on('click',function(){

		FB.ui(

		{

		  method: 'share',

		  href: 'http://warriors.flickkick.co.nz',

		}, function(response){

			 ga('send', 'event', 'game stats', 'share fb', 1);

		});

	});

	$('#signup_fb_login_btn').on('click',function(e){

		e.preventDefault();
		//alert("go login with fb");

		FB.login(function(response) {
		  if (response.status === 'connected') {
		    //$.logThis("Logged into your app and Facebook.");

		    ga('send', 'event', 'game stats', 'signup connect fb', 1);
		    
		    FB.api('/me', function(response) {

		      $.logThis('logged in : Name :> ' + response.name + ' :: user email :> ' + response.email + " :: id :> "+response.id);

		      $("#signup-form input[name='fb_id']").val(response.id);

		      $("#signup-form input[name='name_signup']").val(response.name);

		      $("#signup-form input[name='email_signup']").val(response.email);

		      $("#signup-form input[name='pword_signup']").removeAttr("required" ).hide();

		    });

		  } else if (response.status === 'not_authorized') {
		    $.logThis("The person is logged into Facebook, but not your app.");
		    ga('send', 'event', 'game stats', 'connect fb failed', 1);

		  } else {
		    $.logThis("The person is not logged into Facebook");
		  }
		}, {scope: 'public_profile,email'});

	});

	$('#login_fb_login_btn').on('click',function(e){

		e.preventDefault();

		loadNextStage("#stage2b","#loading");

		FB.login(function(response) {
		  if (response.status === 'connected') {
		    //$.logThis("Logged into your app and Facebook.");

		    FB.api('/me', function(response) {

				$.logThis('logged in : Name :> ' + response.name + ' :: user email :> ' + response.email + " :: id :> "+response.id);

				$("#login-form input[name='fb_id']").val(response.id);

				var formdata = $('#login-form .serial').serialize();

				$.post(

					'includes/user_login.php',

					{
						formdata:formdata
					},

					function(data){

						if(data.status){

							$.logThis("success | user :> "+data.user_id+" :: user name :> "+data.user_name);

							$('.uName').html(data.user_name);

							user.uname = data.user_name;
							user.uid = data.user_id;

							newUser = false;
							
							loadNextStage("#loading","#stage3");
							loadSelectionScreen(0);

							ga('send', 'event', 'game stats', 'user login', 1);

						}else{

							$('#alert_text').html("");

							loadNextStage("#loading","#stage2b");

							if(data.error == "mat"){
								
								$('#alert_text').html("<p>Sorry we don't seem to have you in our system, please sign up</p>");

								$('#alertModal').foundation('reveal', 'open');

							
							}else{

								$('#alert_text').html("<p>encounted an issue ERROR:02</p>");
								$('#alertModal').foundation('reveal', 'open');

							}

						}

					},

					'json'

				);

		    });

		  } else if (response.status === 'not_authorized') {
		  	loadNextStage("#loading","#stage2b");
		    $.logThis("The person is logged into Facebook, but not your app.");

		  } else {
		  	loadNextStage("#loading","#stage2b");
		    $.logThis("The person is not logged into Facebook");
		  }
		});

	});

	$('#select_left').on('click',function(){

		$.logThis("left :> "+playerTicker);

		if(playerTicker == 0){

			playerTicker = num_players-1;

		}else{

			playerTicker --;

		}
		
		loadSelectionScreen(1);

	});

	$('#select_right').on('click',function(){

		$.logThis("right");

		if(playerTicker == (num_players-1)){

			playerTicker = 0;

		}else{

			playerTicker ++;

		}

		loadSelectionScreen(1);

	});

	$('#signup-form').on('invalid.fndtn.abide', function(){

		var invalid_fields = $(this).find('[data-invalid]');
		
		$.logThis(invalid_fields);

	}).on('valid.fndtn.abide', function(){

		$.logThis("Signup Valid!");

		var formdata = $('#signup-form .serial').serialize();

		$.post(

			'includes/user_signup.php',

			{
				formdata:formdata
			},

			function(data){

				if(data.status){

					$.logThis("success | user :> "+data.user_id+" :: user name :> "+data.user_name);

					$('.uName').html(data.user_name);

					user.uname = data.user_name;
					user.uid = data.user_id;

					loadNextStage("#stage2a","#stage3");

					loadSelectionScreen(0);

				}else{

					if(data.error == "dup"){
						
						$('#alert_text').html("<p>Sorry we already have your email in the system. please use another email or sign up using facebook.</p>");

						$('#alertModal').foundation('reveal', 'open');

					
					}else{

						$('#alert_text').html("<p>encounted an issue ERROR:01</p>");
						$('#alertModal').foundation('reveal', 'open');

					}

				}

			},

			'json'

		);

	});

	$('#login-form').on('invalid.fndtn.abide', function(){

		var invalid_fields = $(this).find('[data-invalid]');
		
		$.logThis(invalid_fields);

	}).on('valid.fndtn.abide', function(){

		$.logThis("Login Valid!");

		var formdata = $('#login-form .serial').serialize();

		$.post(

			'includes/user_login.php',

			{
				formdata:formdata
			},

			function(data){

				if(data.status){

					$.logThis("success | user :> "+data.user_id+" :: user name :> "+data.user_name);

					$('.uName').html(data.user_name);

					user.uname = data.user_name;
					user.uid = data.user_id;

					/*if(mobile){
		
						$("#game").fullScreen(true);

					}*/


					
					loadNextStage("#stage2b","#stage3");
					loadSelectionScreen(0);

				}else{

					$('#alert_text').html("");

					if(data.error == "mat"){
						
						$('#alert_text').html("<p>Your login details apear to be incorrect. Please try again.</p>");

						$('#alertModal').foundation('reveal', 'open');

					
					}else{

						$('#alert_text').html("<p>encounted an issue ERROR:02</p>");
						$('#alertModal').foundation('reveal', 'open');

					}

				}

			},

			'json'

		);

		

	});

	$('#signup_back_btn').on('click',function(){

		$.logThis("back");

		loadNextStage("#stage2a","#stage2");

	});

	$('#login_back_btn').on('click',function(){

		$.logThis("back");
		
		loadNextStage("#stage2b","#stage2");

	});

};

function logPlay(){
	$.post(

		'includes/log_play.php',
		
		{

			avartar:players[playerTicker].name,
			avartarId:playerTicker+1,
			nonce:nonce

		},

		function(data){

			if(data.status){

				user.pid = data.play_id;

				$.logThis("pid :> "+user.pid);

			
				loadNextStage("#loading","#stage4");

				launchGame();


			}else{

				$('#alert_text').html("<p>encounted an issue ERROR:03</p>");
				$('#alertModal').foundation('reveal', 'open');

			}

		},

		'json'

	);
}

function loadSelectionScreen(mode){

	if(mode == 0){

		$('.player_select .avartar').html("<img src='img/"+players[playerTicker].src+"' alt='"+players[playerTicker].name+"' />");


		$('#pname').html(players[playerTicker].name);

		positionInitScreen();


	}else{

		$('#pname').hide();

		$('.player_select .avartar').fadeOut(500,function(){

			$('.player_select .avartar').html("<img src='img/"+players[playerTicker].src+"' alt='"+players[playerTicker].name+"' />").fadeIn(500,function(){

					$('#pname').html(players[playerTicker].name).fadeIn(500);

					positionInitScreen();

				}
			);

			

		});

	}

};

function launchGame(){

	$('#game_container').show();
	$('#action_layer').show();

	winWidth = $("#container").innerWidth();
	winHeight = $("#container").innerHeight();

	gameOb = new flickKick();
	gameOb.createGame(winWidth, winHeight);

	if(newUser){

		launchTutorial();

	}else{

		gameOb.launchTimer();

	}

};

function launchTutorial(){

	$('.tuts').css('top',tut_top);
	$('.tuts').css('bottom','auto');

	$('#tutorial_container').show();

	$('.next_tut').on('click',function(){

		//input[name='fb_id']

		$(".activeStep").hide().removeClass("activeStep");

		curTut ++;

		if ($( ".tut_step[data-order='"+curTut+"']" ).length ) {

			$('.tuts').css('text-align',$( ".tut_step[data-order='"+curTut+"']" ).attr('data-align'));

			$.logThis("align :> "+$( ".tut_step[data-order='"+curTut+"']" ).attr('data-align'));

			switch($( ".tut_step[data-order='"+curTut+"']" ).attr('data-vert')){

				case "top":

				$('.tuts').css('top',tut_top);
				$('.tuts').css('bottom','auto');

				break;

				case "bottom":

				$('.tuts').css('top','auto');
				$('.tuts').css('bottom',tut_bottom);

				break;

				case "center":
				$('.tuts').css('top',winHeight/3);
				$('.tuts').css('bottom','auto');
				break;

			}

			$(".tut_step[data-order='"+curTut+"']").addClass("activeStep");

		}else{

			$('#tutorial_container').hide();

			gameOb.launchTimer();

		}

	});

	$('.skip_tut').on('click',function(){

		$('#tutorial_container').hide();
		gameOb.launchTimer();


	});



};

function setupHammer(){

	mc.get("swipe").set({ direction: Hammer.DIRECTION_ALL, velocity:0});


	mc.on("swipe", function(ev) {

    	kickDisabled = true;
    
    	gameOb.launchKick(ev);
		

	});

}

function positionInitScreen(){

	winWidth = $("#container").innerWidth();
	winHeight = $("#container").innerHeight();

	$(".current .init_info").css('margin-top', ((winHeight - $(".current .init_info").outerHeight())/2)*0.8);

}

function resizeListeners(){

	$(window).on('resize',function(){

		winWidth = $("#container").innerWidth();
		winHeight = $("#container").innerHeight();

		positionInitScreen();
		

	});

	$(window).on('orientationchange',function(){

		positionInitScreen();

		$('#rotate_warning').show();

	});

	

}


// =====================================================================================================
// ADDITIONAL FUNCTIONS
// =====================================================================================================
// validate email

function validateEmail(email){ 

	var reg = /^([A-Za-z0-9_\-\.])+\@([A-Za-z0-9_\-\.])+\.([A-Za-z]{2,4})$/; 
	
	if(!reg.test(email)){ 
	
		return false;
	
	}else{
		
		return true;
		
	} 

} 

function getUniqueTime() {
	var time = new Date().getTime();
	while (time == new Date().getTime());
	return new Date().getTime();
}


//IE placehoder input fix
function placeholderFix(){

    //ie placeholder fix
    $.support.placeholder = ( 'placeholder' in document.createElement('input') );
	
	if($.support.placeholder){
		
		$('.form_label').hide();
	}

}
// resize event


// CONSOLE LOG FUNCTION ---------------------------------------------
// taken from http://www.nodans.com/index.cfm/2010/7/12/consolelog-throws-error-on-Internet-Explorer-IE

jQuery.logThis = function(text){
  
   if((window['console'] !== undefined)){
     
        console.log(text);
    
   }

}

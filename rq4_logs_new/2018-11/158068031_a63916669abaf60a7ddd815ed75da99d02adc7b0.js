$(document).ready(function() {

    $("#typing").hide();
    var n = "You: ";
	var default_user = "Guest";
	var gen_about = "Gems ";
    var output_window = $('.chatList');
    var t = $('#typing');
    var i = -1;
	var c = -1;
	var color_variable = 1;
	var number_arrays = 32;
	var number_colors = 12;


	function randomIntFromInterval( min,max )
	{
		return Math.floor(Math.random()*(max-min+1)+min);
	}

      // Arrays
      var msg = ['Thank you mateee! your hack is working boyyy',
				 'does this work?',
				 'do you have facebook website? i want to follow you',
				 'Of course it is working :D',
				 'just testing',
				 'hello guys lets generateeee',
				 'did you start generator?',
				 'hahaha I love this generator in 2 sec I generated unlimited Gems bro',
				 'Whats up mates?',
				 'I just tested and generated 20 000 Gems thanks',
				 'I generated today some gems points, can I generate it one more time today??',
				 'who made this freaking website????',
				 'i tought this is scam, but I generated it and suprise',
				 'Thank you lol I am so happy :D',
				 'I am 11 years old and I love this website, I will show it to mum hehe :DD',
				 'Does Gems points help you in this game a lot?',
				 'I generated today 50000 Gems points and bough a lot of good items!',
				 'How much do you play it daily?',
				 'Who is playing this game?',
				 'can I shutdown game now??',
				 'Hello anybody want to send him Gems ? I generated it a lot of :D',
				 'where I can contact you to donate? I love you !!',
				 'this is working i cant belive juhuuu',
				 'happy new year lol i am just kidding happy life with a lot of Gem generated',
				 'Awesome, it is rare to find working generator like this one',
				 'I can imagine this must be annoying for the one who play with skill',
				 'bye guys, already finish my survey easy, and resources generated successfully',
				 'saw this on forums pretty impressive, hello from girl!',
				 'What about ban secure????',
				 'Cool :D :) <3',
				 'its to protect from spamming, first try to use, i got no Survey request, but for second try i need to get Finish 1 Survey',
				 'i cant imagine this lollll'];

	  var user = ['441Binder','Johnny', 'Attacklord_bro', 'pewdiepie', "Nemanja", "Borislav", "Ivan", "xxMine", "9814Bind", "P!xel", "haxhack", "EarthHacker", "9/4gen", "ClashHacker", 'new_kid',
				  'BloodMaster', 'lovegaming', 'Johnny', 'NaneK', 'Marko_991', 'JackHammer9999', 'fuzzzy', 'tester', 'yearboy1007', 'Ministar', 'Haxajmo', 'Pottato', 'GirlHackingxD', 'Alex',
				  'Coder', 'Hillck23', 'Creeper'];

	  var colors = ['#4286f4',
					'#3ee0ba',
					'#dde540',
					'#e540e2',
					'#e54840',
					'#5401ad',
					'#01ad15',
					'#d15000',
					'#000cf9',
					'#ef5d86',
					'#ef775c',
					'#c5d37e'];


	$( ".livechatSubmtBtn" ).click(function() {
		SendChatMessage();
	});

	$( ".livechatUserGenSubmtBtn" ).click(function() {

		if( $('.livechatUsernameGen').val() == "" ) {

		} else {

			default_user = $('.livechatUsernameGen').val();
			$( "#enter_username" ).hide();
			$( "#send_message" ).show("slow");
		}

	});

	$( ".livechatMsg" ).keydown( function( e ) {

		if ( e.keyCode == 13 ) { SendChatMessage(); }

    });

	setInterval( function(){ runAI();  }, 4500 );
	setInterval( function(){ runGM(); }, 11500 );

	function SendChatMessage() {

		var chat_input_msg = $('.livechatMsg').val();

		if( chat_input_msg == "" ) {

		} else {

			output_window.append( "<li><span class='name' style='color: red;'>" + default_user + "</span>: <span class='message'>" +  chat_input_msg +"</span></li>" );
			$(".livechatListArea").stop().animate({ scrollTop: $( ".livechatListArea" )[ 0 ].scrollHeight }, 1000 );
			$('.livechatMsg').val( '' );
		}

		setTimeout( function(){ CheckChatMessage( chat_input_msg ); }, 6500 );

	}

	function CheckChatMessage( variable ) {

		var random_id = Math.floor((Math.random() * number_arrays) + 1);


		var check_word = "work";

		if( variable.indexOf( check_word ) != -1 ) {
			output_window.append( "<li class='chat-response-msg'><span class='name' style='color: green;'>" + user[ random_id ] + "</span>: <span class='message'>Of course it is working <b>" + default_user + "</b></span></li>" );
		}

		var check_word = "survey";

		if( variable.indexOf( check_word ) != -1 ) {
			output_window.append( "<li class='chat-response-msg'><span class='name' style='color: green;'>" + user[ random_id ] + "</span>: <span class='message'>Yeah, but not scam surveys type :) <b>" + default_user + "</b></span></li>" );
		}

		var check_word = "ban";

		if( variable.indexOf( check_word ) != -1 ) {
			output_window.append( "<li class='chat-response-msg'><span class='name' style='color: green;'>" + user[ random_id ] + "</span>: <span class='message'>you cant get ban with this ;) <b>" + default_user + "</b></span></li>" );
		}

		var check_word = "real";

		if( variable.indexOf( check_word ) != -1 ) {
			output_window.append( "<li class='chat-response-msg'><span class='name' style='color: green;'>" + user[ random_id ] + "</span>: <span class='message'>Yeah!!! This is not dream it is real hahaha <b>" + default_user + "</b></span></li>" );
		}
		$(".livechatListArea").stop().animate({ scrollTop: $( ".livechatListArea" )[ 0 ].scrollHeight }, 1000 );

	}

    function runAI() {

		i = i + 1;
		c = c + 1;

		if( i == number_arrays ) i = 1;
		if( c == number_colors ) c = 1;

		setTimeout(function(){

			$("#typing").show();

		}, 1000);

		setTimeout(function(){
			output_window.append( "<li><span class='name' style='color: " + colors[ c ] + ";'>" + user[ i ] + "</span>: <span class='message'>" + msg[ i ] + "</span></li>" );

			$(".livechatListArea").stop().animate({ scrollTop: $( ".livechatListArea" )[ 0 ].scrollHeight }, 1000 );

		}, 3000);

		setTimeout(function(){

			$("#typing").hide();

		}, 3000);

		$( '.typing-username' ).html( user[ i ] );

    }

    function runGM() {

		random_number = ( parseInt( Math.floor((Math.random() * 10000) + 1000) / 1000, 10 ) +1 ) * 10000;

		setTimeout(function(){
			output_window.append( "<li><b class='chat_animated-text'>SERVER GENERATOR:</b> Generated now " + random_number + " " + gen_about + " by <span class='name chat_animated-text' style='color: " + colors[ c ] + ";'>" + user[ i ] + "</span></li>" );
			$(".livechatListArea").stop().animate({ scrollTop: $( ".livechatListArea" )[ 0 ].scrollHeight }, 1000 );
			$('#chat_buzzer').get( 0 ).play();
		}, 3000);

    }

});
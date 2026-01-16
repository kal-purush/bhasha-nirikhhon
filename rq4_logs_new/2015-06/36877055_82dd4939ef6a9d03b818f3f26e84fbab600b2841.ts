///<reference path='../libs/socket.io-client.d.ts' />
///<reference path='../libs/jquery.d.ts' />


$(document).ready(function(){
	var socket = io();
	socket.on('roomlist', function(roomlist){
		for (var idx = 0; idx < roomlist.length; idx++){
			$('#rooms').append($('<li class="room">').text(roomlist[idx]));
		}
	});	
	$('form').submit(function(){
		location.href = "http://nodejs-basicchat.rhcloud.com/chat/" + $("#n").val();
		event.preventDefault();
	});
	$('#rooms').on('click', 'li', function(){
		var $li = $(this);
		console.log($li.text());
		var index = $li.text().indexOf(":");
		location.href = "http://nodejs-basicchat.rhcloud.com/chat/" + $li.text().substring(0, index);
	});
});
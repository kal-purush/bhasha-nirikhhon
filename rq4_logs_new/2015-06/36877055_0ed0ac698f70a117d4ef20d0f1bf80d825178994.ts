///<reference path='../libs/socket.io-client.d.ts' />
///<reference path='../libs/jquery.d.ts' />			
			
			
			var namespace = window.location.pathname;
			var username = null;
			$(document).ready(function(){
				$('#username').submit(function(){
					var free = true;
					$('li.user').each(function(index){
						if ($(this).text().toLowerCase() === $('#u').val().toLowerCase()){
							$('#messages').append($('<li>').text("That name is taken!"));
							free = false;
						}
					});
					if (free){
						socket.emit('new name', $('#u').val(), username);
						username = $('#u').val();
						$('#welcome').text("Welcome, " + username);
					}
					return false;
				});
				var socket = io(namespace);
				$('#chat').submit(function(){
					if (username != undefined){
						socket.emit('chat message', username + ": " + $('#m').val());
					} else {
						$('#messages').append($('<li>').text("Please choose a username!"));
					}
					$('#m').val('');
					return false;
				});
				socket.on('chat message', function(msg){
					$('#messages').append($('<li>').text(msg));
				});
				socket.on('usernames', function(names){
					for (var idx = 0; idx < names.length; idx++){
						if(names[idx] != null){
							$('#users').append($('<li class="user">').text(names[idx]));
						}
					}
					var $users = $('#users');
					var $usersli = $users.children();
					$usersli.sort(function(a, b){
						var atext = $(a).text().toLowerCase();
						var btext = $(b).text().toLowerCase();
						if (atext> btext){ return 1;}
						else if (atext< btext){return -1;}
						else {return 0;}
					});
					$usersli.detach().appendTo($users);
				});
				socket.on('new name', function(name, oldname){
					if(oldname === null){
						$('#messages').append($('<li>').text(name + " has joined the room"));
					} else {
						$('#messages').append($('<li>').text(oldname + " is now called " + name));
					}
					$('li.user').each(function(index){
						if ($(this).text() === oldname){
							$(this).remove();
						}
					});
					$('#users').append($('<li class="user">').text(name));
					var $users = $('#users');
					var $usersli = $users.children();
					$usersli.sort(function(a, b){
						var atext = $(a).text().toLowerCase();
						var btext = $(b).text().toLowerCase();
						if (atext> btext){ return 1;}
						else if (atext< btext){return -1;}
						else {return 0;}
					});
					$usersli.detach().appendTo($users);
				});
				socket.on('userGone', function(name){
					if(name != null){
						$('#messages').append($('<li>').text(name + " left the room"));
						$('li.user').each(function(index){
							if ($(this).text() === name){
								$(this).remove();
							}
						});
					}
				});
			});
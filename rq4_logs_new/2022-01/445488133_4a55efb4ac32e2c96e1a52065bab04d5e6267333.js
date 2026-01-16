$(async function() {
  // get admins
  const admins = await fetch("/admins.json").then(data => data.json()).then(res => res.admins);

  // conveniently edit taskbar info, even if its messy
  var version = '1.4';
  var whatsNew = `
    - Message display overhaul!!`;
  var info = `
  <a href="https://cutt.ly/ihproject" target="_blank">Other Projects</a>
  <br>
  <span id="suggest" style="color:#663366;text-decoration:underline;cursor: pointer;">Suggestion Form</span>
  <br>
  <span id="gameButton" style="color:#663366;text-decoration:underline;cursor: pointer;">Pong Test! (embedded)</span>
  <br><br>
  Admins:  `+ admins.map(adm => adm.name).toString().split(',').join(', ') + `
  <br><br>
  Enter "/cmds" to see a list of commands.
  <br>
  Type "@" before a username to send a notification! ('@username')
  <br>
  Paste an image on your clipboard (while focusing on input box) to send an image.
  <br><br>
  If you find a bug, make sure to contact me by adding a comment to the repl's spotlight page.
  `

  $('#tbinfo').html('<div id="updates">' + whatsNew + '<br></div><br><div id="misc">' + info + '</div><notice id="tbNotice" class="vue"></notice>');

  // change audio file volume 
  $('#notif2').prop('volume', 0.2);

  $("#gameButton").on('click', function() {
    coolTransition({
      element: "#game",
      visibility: "show"
    });
  });
  $("#game span").on("click", function() {
    coolTransition({
      element: "#game",
      visibility: "hide"
    });
  });

  $('#suggest').on('click', function() {
    // open suggestions form
    Swal.fire({
      html: `<iframe src="https://docs.google.com/forms/d/e/1FAIpQLSf5pRKmNufacsOIsuXovmasweON4qykxip6d9Sc0-CRQOxyNg/viewform?embedded=true" width="640" height="455" frameborder="0" marginheight="0" marginwidth="0">Loading…</iframe>`,
      width: '800px'
    });
  });

  function showRules() {
    // open rules display
    Swal.fire({
      title: 'Rules',
      html: `<rules style="list-style-type: &quot;👉&quot;;text-align:left;"><li> Don't use profane language.</li><li> Don't try to abuse any exploits.</li><li> Don't spam.</li><li> Use common sense.</li></rules><br>`,
      icon: 'info',
      width: '35%'
    })
  }
  function showFaq() {
    // open FAQ display
    Swal.fire({
      title: 'FAQ',
      html: `Q: <i>Who created this?</i><br>A: Who knows? I don't.. do you?<br><br>Q: <i>Why was this created?</i><br>A: This was created simply for fun!<br><br>Q: <i>How can I submit a suggestion?</i><br>A: There is a suggestion form in the webchat's taskbar.`,
      icon: 'info',
      width: '30%'
    })
  }
  function showInfo() {
    // show info display
    Swal.fire({
      title: 'Credit',
      html: `Created with <a href="https://repl.it" target="_blank">repl.it</a><br>Created by @7ih on Github, Replit. Find more projects by 7ih <a href="https://projects.7ih.repl.co/" target="_blank">here</a>!<br><br><cred style="text-align:left;"><span style="position:relative;color:red;float:left;"><i>code:</i></span><br><li><a href="https://github.com/paolodelia99/Simple-node-chat-app/tree/master/client" target="_blank">Inspiration for the username input</a></li><li><a href="https://github.com/socketio/chat-example" target="_blank">Basic chat functions and helpful example</a></li><li><a href="https://github.com/mmukhin/psitsmike_example_2" target="_blank">Helpful example for adding multiple rooms</a></li><br><span style="position:relative;color:red;float:left;"><i>main plugins:</span><br></a><li>peerjs</li><li>sweetalert2</li><li>jquery</li><li>socket.io</li></cred>`,
      icon: 'info',
      width: '40%'
    })
  }

  function playId(id) {
    document.getElementById(id).play();
    document.getElementById(id).currentTime = 0;
  }

  var shades = document.querySelectorAll('.backgrounds-shade');
  function coolTransition(prop) {
    shades.forEach(function(i) {
      setTimeout(function() {
        i.style.visibility = "visible";
        i.style.transform = "scaleX(1) scaleY(1)";
      }, 300 * Math.random());
    });
    setTimeout(function() {
      if (prop.visibility === "show") $(prop.element).show();
      else if (prop.visibility === "hide") $(prop.element).hide();
      shades.forEach(function(i) {
        i.style.transform = "scaleX(0) scaleY(1)";
      });
    }, 1500);
    setTimeout(function() {
      shades.forEach(function(i) {
        i.style.transition = "all 0ms linear";
        i.style.transform = "scaleX(1) scaleY(0)";
      });
    }, 2500);
    setTimeout(function() {
      shades.forEach(function(i) {
        i.style.visibility = "hidden";
        i.style.transition = "";
      });
    }, 2600);
  }

  async function sendImage(file) {
    function compressImg() { return new Promise(resolve => {
      new Compressor(file, {
        quality: 0.3,
        maxWidth: 950,
        minWidth: 50,
        mimeType: 'image/webp',
        success(result) {
          resolve(result);
        },
        error(err) {
          console.log(err.message);
        },
      });
    });}
    var reader = new FileReader();
    reader.onload = function(e) {
      Swal.fire({
        title: 'Send an Image',
        input: 'textarea',
        inputLabel: (isPm ? 'Private Message' : 'Message'),
        inputPlaceholder: 'Type your message here...',
        inputAttributes: {
          onkeydown: "return !/^[<>]+$/ig.test(event.key)",
          onpaste: "noXSS(this)"
        },
        showCancelButton: true,
        imageUrl: e.target.result,
        imageAlt: 'image',
        confirmButtonText: 'Send',
      }).then((result) => {
        if (result.isConfirmed) {
          var img = Base64String.compress(e.target.result.split('base64,')[1]);
          var msg = result.value + ' <br> <img>';
          if (isPm) // private message
            socket.emit('pm', pmUser, msg, img);
          else if (admin)
            socket.emit('admin message', tag + username + ' : ' + msg, adminColor, img);
          else
            socket.emit('chat message', tag + username + ' : ' + msg, img);
        }
      });
    }
    var file = await compressImg();
    reader.readAsDataURL(file);
  }

  // prevent user from pasting "<>" (prevents html manipulation)
  window.noXSS = function(el) {
    setTimeout(function() {
      $(el).val($(el).val().replace(/[<>]+/gi, ''));
    });
  }

  $('#m').on('paste', async function() {
    noXSS('#m');

    // paste images (data url is 'e.target.result')
    var items = (event.clipboardData || event.originalEvent.clipboardData).items;
    for (var i in items) {
      item = items[i];
      if (item.kind === 'file') sendImage(item.getAsFile());
    }
  });

  $("#addImage").click(function() {
    Swal.fire({
      title: 'Attach an Image',
      input: 'file',
      showCancelButton: true,
      confirmButtonText: 'Attach',
    }).then(res => {
      var file = res.value;
      if (!file.type.includes("image")) {
        Swal.fire({
          icon: 'error',
          title: 'Well, this is awkward.',
          text: "Soo... you were supposed to send an image.",
          confirmButtonText: 'Send',
        });
      } 
      sendImage(file);
    });
  });

  function closeFirstMenu() {
    coolTransition({
      element: "#myModal",
      visibility: "hide"
    });
  }
  function openFirstMenu() {
    coolTransition({
      element: "#myModal",
      visibility: "show"
    });
    setTimeout(function() {
      $('#logo').css('top', '25px');
    }, 1000);
  }

  // declare variables related to username and stuff
  var socket = io();
  var username;
  var tmpUsername;
  var noname;
  var admin = 0;
  var banned = 0;

  var start = 0;
  var ping = 0;
  var vcid;
  var myStream;

  // voicechat stuff
  var peer = new Peer();
  peer.on('open', function(id) {
    vcid = id;
  });

  // mute button in vc menu
  $('#mute').on('click', function() {
    if (myStream.getTracks()[0].enabled) {
      $('#mute').html('mic_off');
    } else if (!myStream.getTracks()[0].enabled) {
      $('#mute').html('mic');
    }
    myStream.getTracks().forEach(track => track.enabled = !track.enabled);
  });

  // leave button for vc menu
  $('#leave').on('click', function() {
    try {
      call.close();
    } catch (err) {
      Toast.fire({
        icon: 'error',
        title: 'Error with leaving the call. ' + err
      });
    };
  });

  // detect if user is banned
  if (document.cookie.split(';').some((item) => item.includes('banned=1'))) {
    Swal.fire(
      'You are banned.',
      'If this is a mistake, you can get unbanned.',
      'error'
    );
    playId('e')
    id = Math.floor(Math.random() * (999 - 100 + 1) + 100);
    username = 'BANNEDUSER' + id;
    socket.emit('adduser', username, 'banned');
    $('#nickname-input').prop('disabled', false);
    closeFirstMenu();
    banned = 1;
  }

  // prevent context menu actions in username input
  $('#nickname-input').on("cut copy paste", function(e) {
    e.preventDefault();
    Toast.fire({
      icon: 'error',
      title: 'That isn\'t allowed.'
    });
  });

  // open/close acc menu 
  $('body').on('click', function(e) {
    if (!$(e.target).closest('.acc').length) {
      $('#menuAcc').stop().animate({right: '1%', opacity: 0},function(){$(this).hide();});
    } else if ($('#menuAcc').css('opacity') == 0) {
      $('#menuAcc').stop().show().animate({right: '5%', opacity: 1});
    }
  });
  $('#openAcc').on('click', function() {
    if ($('#menuAcc').css('opacity') != 0) {
      $('#menuAcc').stop().animate({right: '1%', opacity: 0},function(){$(this).hide();});
    }
  });

  $('#menuAcc').on('keypress', '#tagAcc', function(e) {
    if ((e.keyCode || e.which) == '13') { 
      if ($('#tagAcc').val()) {
        tag = '[' + $('#tagAcc').val() + '] '
        Toast.fire({
          icon: 'success',
          title: 'You changed your tag to ' + $("#tagAcc").val()
        });
      } else {
        tag = '';
        Toast.fire({
          icon: 'success',
          title: 'You removed your tag'
        });
      }
    }
  });

    $('#menuAcc').on('keypress', '#userAcc', function(e) {
    if ((e.keyCode || e.which) == '13') {
      let userinput = $('#userAcc').val();
      if (userinput) {
        // admin
        if (userinput.length > 10) {
          // no hackers.
          Swal.fire(
            'Nice try.',
            'You aren\'t special.',
            'error'
          );
          playId('e');
          tmpUsername = 'FORTNITE!!';
          adduser();
        } else if (userinput.toLowerCase().includes("server") || admins.includes(userinput.toLowerCase()) || userinput.toLowerCase().includes("eames") || userinput.toLowerCase().includes("nig")) {
          // honka honka
          tmpUsername = '🤡';
          adduser();
        } else {
          tmpUsername = userinput;
          adduser();
        }
        noname = 0;
      } else {
        id = Math.floor(Math.random() * (999 - 100 + 1) + 100);
        tmpUsername = 'USER' + id;
        adduser();
        noname = 1;
      }
    }
    function adduser() {
      // submit user to server
      tmpUsername = tmpUsername.trim().replace(/\s\s+/g, ' ');
      socket.emit('nameChange', tmpUsername, function nameTaken(taken) {
        if (taken === true) {
          // if requested username was already taken
          if (noname) {
            id = Math.floor(Math.random() * (999 - 100 + 1) + 100);
            tmpUsername = 'USER ' + id;
            adduser();
          } else {
            Swal.fire(
              'This name isn\'t allowed.',
              '"' + tmpUsername + '" is already being used. Please choose another username.',
              'error'
            );
            playId('e');
            $('#userAcc').val(username);
          }
        } else if (taken == 'e') {
          Swal.fire(
              'This name isn\'t allowed.',
              'Your username is already ' + tmpUsername,
              'error'
            );
          playId('e');
        } else {
          username = tmpUsername;
        }
      });
    }
  });

  // get ping
  start = Date.now();
  socket.emit('ping', function clientCallback() {
    ping = Date.now() - start;
    $('#ping').text('Ping: ' + ping + 'ms');
  });
  setInterval(function() {
    start = Date.now();
    socket.emit('ping', function clientCallback() {
      ping = Date.now() - start;
      $('#ping').text('Ping: ' + ping + 'ms');
    });
  }, 10000)

  // custom toast notif settings
  const Toast = Swal.mixin({
    toast: true,
    position: 'top-end',
    showConfirmButton: false,
    timer: 3000,
    timerProgressBar: true,
    didOpen: () => {
      playId("n3");
    }
  });

  // hash security function
  String.prototype.hashCode = function() {
    var hash = 0;
    if (this.length == 0) {
      return hash;
    }
    for (var i = 0; i < this.length; i++) {
      var char = this.charCodeAt(i);
      hash = ((hash << 5) - hash) + char;
      hash = hash & hash;
    }
    return hash;
  }

  // when you the idk username thing
  $('#nickname-input').keypress(function(e) {
    $('#menuAcc').html('<input id="userAcc" maxlength="10" placeholder="username" style="width: 200px;"><li style="font-size: 18px;">press enter to submit</li>');
    let keycode = (e.keyCode ? e.keyCode : e.which);
    if (keycode == '13') {
      let userinput = $('#nickname-input').val();
      if (userinput) {
        // admin
        if (userinput.charAt(0) == '!') {
          for (let i = 0; i < admins.length; i++) {
            var adm = admins[i];
            if (userinput.hashCode() === adm.password) {
              username = adm.name;
              adminColor = adm.color;
              adminUser();
            }
          }
        } else if (userinput.length > 10) {
          // no hackers.
          Swal.fire(
            'Nice try.',
            'You aren\'t special.',
            'error'
          );
          playId('e')
          username = 'FORTNITE!!';
          adduser();
        } else if (userinput.toLowerCase().includes("server") || admins.includes(userinput.toLowerCase()) || userinput.toLowerCase().includes("eames") || userinput.toLowerCase().includes("nig")) {
          // honka honka
          username = '🤡';
          adduser();
        } else {
          username = userinput;
          admin = 0;
          adduser();
        }
        noname = 0;
      } else {
        id = Math.floor(Math.random() * (999 - 100 + 1) + 100);
        username = 'USER' + id;
        admin = 0;
        adduser();
        noname = 1;
      }
    }
    function adduser() {
      // submit user to server
      $('#nickname-input').prop('disabled', true);
      username = username.trim().replace(/\s\s+/g, ' ');
      socket.emit('adduser', username, admin, vcid, function nameTaken(taken) {
        if (taken) {
          // if requested username was already taken
          if (noname) {
            id = Math.floor(Math.random() * (999 - 100 + 1) + 100);
            username = 'USER ' + id;
            adduser();
          } else {
            $('#nickname-input').prop('disabled', false);
            Swal.fire(
              'This name isn\'t allowed.',
              '"' + username + '" is already being used. Please choose another username.',
              'error'
            );
            playId('e');
          }
        } else {
          $('#nickname-input').val('');
          $('#userAcc').val(username);
          closeFirstMenu();
        }
      });
    }
    function adminUser() {
      // admin stuff
      admin = 1;
      $("#m").attr('maxlength', '500');
      $('#menuAcc').html('<input id="tagAcc" maxlength="6" placeholder="tag"><input id="userAcc" maxlength="10" placeholder="username"><li style="font-size: 18px;">press enter to submit</li>');
      adduser();
    }
  });

  // declare variables related to chat messages and commands
  var submissions = 0;
  var spamTimer = 1;

  var input;

  var whitelisted = 0;

  var playing = 0;

  var words;
  var command;
  var target;

  var tag = "";
  var hasTag = 0;
  var adminColor = 'dodgerBlue';

  var typing = false;
  var timeout;
  var usersTyping = [];
  var messageList = document.getElementById('messages');

  var tbScreen = "rooms";

  var isPm = 0;
  var pmUser = '';

  // message function to display messages
  function message(msg, color) {
    var name = msg.split(' : ')[0];
    var nameNoTag = name.includes("] ") ? name.split("] ")[1] : name;
    var msgSender = nameNoTag === username ? "client" : "network";
    msg = msg.substr(name.length);
    var scrollDown = messageList.scrollHeight - messageList.scrollTop <= messageList.clientHeight + 50;
    var biggerMargin = $("#messages :last-child")[$("#messages :last-child").length-2] ? $("#messages :last-child")[$("#messages :last-child").length-2].className !== +msgSender + "Msg" : false;
    $('#messages').append($('<li id="messagesNormal" class="' + msgSender + 'Msg" style="margin: ' + (biggerMargin ? 30 : 10) + 'px ' + (msgSender == "client" ? 100 : 0) + 'px 10px ' + (msgSender == "network" ? 100 : 0) + 'px;">').html('<a style="color:' + color + ';">' + name + '</a><a style="color:#2F4F4F;">' + msg + '</a>'));
    if (scrollDown) messageList.scrollTo(0, messageList.scrollHeight);
  }
  // display info chats
  function smallMessage(msg) {
    if (messageList.scrollHeight - messageList.scrollTop <= messageList.clientHeight + 50) {
      $('#messages').append($('<li id="messagesSmall">').html('<a style="color:#ce2029;">' + msg + '</a>'));
      messageList.scrollTo(0, messageList.scrollHeight);
    } else {
      $('#messages').append($('<li id="messagesSmall">').html('<a style="color:#ce2029;">' + msg + '</a>'));
    }
  }
  function joinRoom(room) {
    // join a room
    $("#messages").empty();
    socket.emit('switchRoom', room);
    $('#feedback').html(" ");
  }

  // stuff related to taskbar opening/closing
  $('body').on('click', function(e) {
    if (!$(e.target).closest('.taskbar').length) {
      $('taskbar').animate({ left: '-20%' }, 'fast');
      $('#taskbarEffect').fadeOut('fast');
    } else {
      $('taskbar').animate({ left: '0' }, 'fast');
      $('#taskbarEffect').fadeIn('fast');
    }
  });

  function changeTbButton(id) {
    if (tbScreen != id) {
      var currentId = tbScreen;
      $('#' + currentId + 'Button').css('background', '#e0ad9d').css("cursor", "pointer");
      $('#' + id + 'Button').css('background', '#45ADA8').css("cursor", "default");
      $('.tb' + currentId).animate({ left: '-100%' }, 'fast', function() {
        $('.tb' + currentId).css("left", '100%');
      });
      $('.tb' + id).animate({ left: '0' }, 'fast');
      tbScreen = id;
    }
  }

  $('#infoButton').on('click', function() {
    changeTbButton('info');
  });
  $('#usersButton').on('click', function() {
    changeTbButton('users');
    socket.emit('reqUserlist', 'taskbar');
  });

  socket.on('userlist', function(userlist) {
    var users = '';
    var extcmds = [];
    for (let i = 0; i < userlist.length; i++) {
      let userStr = userlist[i].toString();
      users += '<div id="' + userStr + '" class="users">' + userStr + '<a class="material-icons" style="font-size:24px;right:0;position:absolute;width:50px">arrow_drop_down</a></div><div id="' + userStr + 'act" class="userAction"><div id="' + userStr + ';vc" class="userActions">Call</div><div id="' + userStr + ';pm" class="userActions">Message</div></div><d></d>';
      if (admin && !Object.values(admins).forEach(adm => adm.name.includes(userStr))) {
        extcmds.push(userStr);
      }
    }
    $('#userslist').html(users);

    $("#" + username.replace(' ', '\\ ')).html('<div id="' + username + '" class="users" style="cursor:default;background:#6fc5a1;">' + username + '</div>')

    for (let i = 0; i < extcmds.length; i++) {
      let userStr = extcmds[i];
      $('#' + userStr.replace(' ', '\\ ') + 'act').html('<div id="' + userStr + ';vc" class="userActions">Call</div><div id="' + userStr + ';pm" class="userActions">Message</div><div id="' + userStr + ';whitelist" class="userActions" style="background: #547980;">Whitelist</div><div id="' + userStr + ';reqMute" class="userActions" style="background: #547980; width: 32.5%;float: left;margin-right:5%;">Mute</div><div id="' + userStr + ';reqUnmute" class="userActions" style="background: #547980; width: 32.5%;float: left;">Unmute</div><div id="' + userStr + ';reqBan" class="userActions" style="background: #547980;width: 32.5%;float: left;margin-right:5%;">Ban</div><div id="' + userStr + ';reqUnban" class="userActions" style="background: #547980;width: 32.5%;float: left;">Unban</div>');
    }

    $('.users').on('click', function() {
      var user = this.id.replace(' ', '\\ ');
      var dir = ($('#' + user + 'act').is(':visible') ? 'down' : 'up');
      $('#' + user + ' a').html('arrow_drop_' + dir);
      $('#' + user + 'act').stop().slideToggle('fast');
    });

    // user actions button thingies 
    $('.userActions').on('click', function() {
      let user = this.id.split(';')[0]; 
      let cmd = this.id.split(';')[1];
      if (cmd == 'pm') {
        isPm = 1;
        pmUser = user;
        $('#pm').css('display', 'block').html('You are currently private messaging ' + pmUser + '<span class="material-icons">cancel</span>');
        $('#pm span').on('click', function() {
          isPm = 0;
          $('#pm').css('display', 'none');
        });
      } else {
        socket.emit(cmd, user);
      }
    });
  });

  $('.rooms').on('click', function() {
    if (!$(this).text().includes('>')) {
      joinRoom(this.id);
      rooms = document.querySelectorAll('.rooms');
      rooms.forEach((room) => {
        $(room).text ($(room).text().replace('> ','').replace(' <',''));
        $(room).css('cursor','pointer');
      });
      $(this).text(`> ${$(this).text()} <`).css('cursor','default');
    }
  });
  $('#roomForm').submit(function() {
    var roomInput = $("#entroom");
    joinRoom(roomInput.val());
    roomInput.val("");
    return false;
  });

  /*

    This message will probably never be seen, but..
    Why the heck are you looking through this awful code??
    Do you find enjoyment out of this????
    Go do something actually productive, nerd! >:(

  */


  $('#roomsButton').on('click', function() {
    changeTbButton('rooms');
  });

  // submit message
  $('#msgForm').submit(function() {
    input = $('#m').val();
    if (input.length > 250 && !admin) {
      input = 'im a stupid hacker xDxD Poggers i like furries and i play fortnite for 16 hours a day'; // no haker!!!!
    }
    if (submissions > 0 && !(admin || whitelisted)) {
      // no spamming
      if (input) {
        Swal.fire(
          `You can't do that.`,
          `Please wait for the Slowmode Timer to run out. {` + spamTimer + ` second(s)}`,
          `error`
        )
      }
    }
    else if (input.charAt(0) == "/") {
      // commands and stuff
      if (banned) {
        Toast.fire({
          icon: 'error',
          title: 'You are not allowed to do this'
        });
      } else {

        words = input.split(' ');
        command = words[0].toLowerCase();
        target = words[1];

        if (command == '/cmds') {
          Swal.fire({
            title: 'Commands',
            html: `<a style="text-align:left;"><h4 style="color:red">note: if a user's name has a whitespace (" ") in it, replace the space with an underscore ("_") when using commands involving usernames<h4><br><h3>normal commands</h3><li>/username</li><li>/userlist</li><li>/pm [someone's username] [a message]</li><li>/room [a room number]</li><li>/vc [someone's username]</li><li>/elevator</li><br><h3>admin commands</h3><li>/clear</li><li>/mute [someone's username]</li><li>/unmute [someone's username]</li><li>/whitelist [someone's username]</li><li>/announce [a message]</li><li>/ban [someone's username]</li><li>/unban [someone's username]</li><br><h3>whitelist commands</h3><li>/tag [a word]</li><li>/warn [username] [reason]</li><li>/createadmin [name] [color] [password]</li><br></a>`,
            icon: 'info'
          })
        }
        
        else if (command == '/clear') {
          if (admin) {
            socket.emit('reqClear');
          } else {
            Toast.fire({
              icon: 'error',
              title: 'You are not an admin!'
            });
          }
        }
        else if (command == '/username') {
          socket.emit('du');
          openFirstMenu();
          $('#nickname-input').prop('disabled', false);
          tag = '';
        }
        else if (command == '/userlist') {
          socket.emit('reqUserlist');
        }
        else if (command == '/mute') {
          if (admin) {
            if (target) {
              if (Object.values(admins).forEach(adm => adm.name.includes(target))) {
                Toast.fire({
                  icon: 'error',
                  title: 'Nice try.'
                });
              } else {
                socket.emit('reqMute', target);
              }
            } else {
              smallMessage('Invalid syntax. Correct usage: /mute [username]');
            }
          } else {
            Toast.fire({
              icon: 'error',
              title: 'You are not an admin!'
            });
          }
        }
        else if (command == '/unmute') {
          if (admin) {
            if (target) {
              if (Object.values(admins).forEach(adm => adm.name.includes(target))) {
                Toast.fire({
                  icon: 'error',
                  title: 'Nice try.'
                });
              } else {
                socket.emit('reqUnmute', target);
              }
            } else {
              smallMessage('Invalid syntax. Correct usage: /unmute [username]');
            }
          } else {
            Toast.fire({
              icon: 'error',
              title: 'You are not an admin!'
            });
          }
        }
        else if (command == '/pm') {
          if (target && words[2]) {
            socket.emit('pm', target, words.splice(2).join(' '));
          } else {
            smallMessage('Invalid syntax. Correct usage: /pm [username] [message]');
          }
        }
        else if (command == '/room') {
          if (target) {
            if (admin || whitelisted) {
              joinRoom(target);
            } else if (/^[0-9]$/.test(target)) {
              joinRoom(target);
            } else {
              Toast.fire({
                icon: 'error',
                title: "You aren't allowed in this room."
              });
            }
          } else {
            smallMessage('Invalid syntax. Correct usage: /room [number]');
          }
        }
        else if (command == '/whitelist') {
          if (admin) {
            if (target) {
              socket.emit('whitelist', target);
            } else {
              smallMessage('Invalid syntax. Correct usage: /whitelist [username]');
            }
          } else {
            Toast.fire({
              icon: 'error',
              title: 'You are not an admin!'
            });
          }
        }
        else if (command == '/elevator') {
          if (playing) {
            document.getElementById('m1').pause();
            playing = 0;
          } else {
            playId('m1');
            playing = 1;
          }
        }
        else if (command == '/tag') {
          if (admin || whitelisted) {
            if (target && !hasTag) {
              tag = '[' + target + '] ';
              hasTag = 1;
            } else if (target && hasTag) {
              tag = '[' + target + '] ';
            } else if (hasTag) {
              tag = '';
              hasTag = 0;
            } else {
              smallMessage('Invalid syntax. Correct usage: /tag [word]');
            }
          } else {
            Toast.fire({
              icon: 'error',
              title: 'You are not an admin!'
            });
          }
        }
        else if (command == '/announce') {
          if (admin || whitelisted) {
            if (target) {
              socket.emit('announce', words.splice(1).join(' '));
            } else {
              smallMessage('Invalid syntax. Correct usage: /announce [message]');
            }
          } else {
            Toast.fire({
              icon: 'error',
              title: 'You are not an admin!'
            });
          }
        }
        else if (command == '/ban') {
          if (admin) {
            if (target) {
              if (Object.values(admins).forEach(adm => adm.name.includes(target))) {
                Toast.fire({
                  icon: 'error',
                  title: 'Nice try.'
                });
              } else {
                socket.emit('reqBan', target);
              }
            } else {
              smallMessage('Invalid syntax. Correct usage: /ban [user]');
            }
          } else {
            Toast.fire({
              icon: 'error',
              title: 'You are not an admin!'
            });
          }
        }
        else if (command == '/unban') {
          if (admin) {
            if (target) {
              if (Object.values(admins).forEach(adm => adm.name.includes(target))) {
                Toast.fire({
                  icon: 'error',
                  title: 'Nice try.'
                });
              } else {
                socket.emit('reqUnban', target);
              }
            } else {
              smallMessage('Invalid syntax. Correct usage: /unban [user]');
            }
          } else {
            Toast.fire({
              icon: 'error',
              title: 'You are not an admin!'
            });
          }
        }
        else if (command == '/vc') {
          if (target) {
            socket.emit('vc', target);
          } else {
            smallMessage('Invalid syntax. Correct usage: /vc [username]');
          }
        }
        else if (command == '/warn') {
          if (admin || whitelisted) {
            if (target) {
              if (Object.values(admins).forEach(adm => adm.name.includes(target))) {
                Toast.fire({
                  icon: 'error',
                  title: 'Nice try.'
                });
              } else {
                socket.emit('reqWarn', target, words.splice(2).join(' '));
              }
            } else {
              smallMessage('Invalid syntax. Correct usage: /warn [user] [reason]');
            }
          } else {
            Toast.fire({
              icon: 'error',
              title: 'You are not an admin!'
            });
          }
        }
        else if (command == '/update') {
          if (admin) {
            socket.emit('reqUpdate');
          } else {
            Toast.fire({
              icon: 'error',
              title: 'You are not an admin!'
            });
          }
        }
        else if (command == '/createadmin') {(async function() {
          if (!words[3]) {
            smallMessage('Invalid syntax. Correct usage: /createadmin [name] [color] [password]');
            return;
          }
          if (!await fetch("/admins.json").then(data => data.json()).then(res => res.allowAdminCreation)) {
            Swal.fire({
              icon: 'error',
              title: "That isn't allowed!",
              html: "You don't have permission to create an admin.<br><br>If you are the owner of the site, read the README file."
            });
            return;
          }
          const adminData = {
            name: target,
            color: words[2],
            password: `!${words[3]}`.hashCode()
          }
          socket.emit("create admin", adminData, function() {
            Swal.fire({
              icon: 'success',
              title: 'Succesfully created admin object',
              html: "<i>Refresh the page to use the new admin data.</i> To login as an admin, enter your username as '!password', with 'password' being your requested password." + (words[3].length > 9 ? "<br><br>Note: since your password can't fit in the default 10 character username input box, you will need to increase the max length. Or just choose a new password." : "")
            });
          });
        })()}
        else {
          smallMessage('Invalid command. Type "/cmds" for a list of commands!');
        }
      }
    } else {
      if (input) {
        if (isPm) socket.emit('pm', pmUser, input);
        else {
          if (admin) socket.emit('admin message', tag + username + ' : ' + input, adminColor);
          else socket.emit('chat message', tag + username + ' : ' + input)
        }
      }
    }

    // prevent spam thingies
    submissions += 1;
    setTimeout(function() {
      submissions -= 1;
      document.getElementById("m").disabled = false;
    }, spamTimer * 1000);
    $('#m').val('');
    // ur not typing if a message has been sent
    notTyping();
    return false;
  });
  socket.on('chat message', function(msg, color, img) {
    // display message from server
    if (img) {
      msg = msg.replace('<img>', '<img class="chat-img" src="data:image/webp;base64,' + Base64String.decompress(img) + '" alt="image">')
    }
    message(msg, color);
  });
  socket.on('sendClear', function() {
    // clear array of messages
    $("#messages").empty();
    smallMessage('The chat was cleared.');
  });
  socket.on('mute', function(idk) {
    // muting function
    if (!admin) {
      user = (idk) ? idk : '{SYSTEM}';
      Swal.fire(
        'You got muted.',
        'You were muted by ' + user,
        'error'
      );
      playId('e');
      document.getElementById("m").disabled = true;
      document.getElementById("m").placeholder = 'You are muted.';
    }
  });
  socket.on('unmute', function(idk) {
    // unmute function
    Swal.fire(
      'You got unmuted.',
      'You were unmuted by ' + idk,
      'success'
    );
    playId('n2');
    document.getElementById("m").disabled = false;
    document.getElementById("m").placeholder = 'type your message here';
  });
  socket.on('ban', function(idk) {
    // ban function
    if (!admin) {
      document.cookie = "banned=1; expires=Tue, 19 Jan 2038 03:14:07 UTC";
      socket.emit('du');
      Swal.fire(
        'You got banned.',
        'You were banned by ' + idk,
        'error'
      ).then(() => {
        location.reload();
      });
      playId('e');
    }
  });
  socket.on('unban', function(idk) {
    // unban function
    if (!admin) {
      document.cookie = "banned=0";
      Swal.fire(
        'You got unbanned.',
        'You were unbanned by ' + idk,
        'success'
      ).then(() => {
        location.reload();
      });
      playId('n2');
    }
  });
  socket.on('pMsg', function(sender, msg) {
    // display private message
    message('{PRIVATE MESSAGE} ' + sender + ' : ' + msg, '#49796b');
    playId('n1');
  });
  socket.once('whitelisted', function(user) {
    // get whitelisted
    whitelisted = 1;
    Swal.fire(
      'You are whitelisted.',
      'You were whitelisted by ' + user + '. Enjoy the few perks of being a whitelisted member!',
      'success'
    );
    playId('n2');
  });
  socket.on('warn', function(warner, reason) {
    // get warned
    if (!admin) {
      reason = (reason) ? reason : 'none.';
      Swal.fire({
        title: 'You got a warning!',
        html: 'You were warned by ' + warner + '.<br><br>Reason: ' + reason,
        icon: 'error'
      });
      playId('e');
    }
  });
  socket.on('sendUpdate', function() {
    // update user by refreshing their page
    Swal.fire({
      title: 'The website has recieved an update!',
      text: 'Reshresh your page?',
      icon: 'info',
      showCancelButton: true,
      cancelButtonText: `im dumb`,
      confirmButtonText: `Yes`,
    }).then((result) => {
      if (result.isConfirmed) {
        location.reload();
      }
    });
    playId('n3');
  });
  // the following is a jumble of inefficient and messy code related to voicechat.
  socket.on('vc', function(vc, user) {
    // this gets activated if you request voicechat
    navigator.mediaDevices.getUserMedia({ video: false, audio: true })
      .then(function(mylocalStream) {
        myStream = mylocalStream;
        call = peer.call(vc, myStream, { metadata: { caller: username } });
        call.on('stream', function(stream) {
          var audio = document.createElement('audio');
          audio.srcObject = stream;
          audio.onloadedmetadata = function() {
            Toast.fire({
              icon: 'success',
              title: user + ' accepted the call.'
            });
            $("#vcMenu").show();
            audio.play();
          }
        });
        call.on('close', function() {
          $('#vcMenu').hide();
          Toast.fire({
            icon: 'error',
            title: 'You have disconnected from ' + user + '.'
          });
          var s = function(t) {
            t.stop();
          }
          myStream.getAudioTracks().map(s);
          socket.removeListener('hungup');
          socket.emit('hangup', user);
        });
        socket.on('hungup', function() {
          $('#vcMenu').hide();
          Toast.fire({
            icon: 'error',
            title: user + ' disconnected from the call.'
          });
          var s = function(t) {
            t.stop();
          }
          myStream.getAudioTracks().map(s);
        });
        socket.on('dec', function() {
          Toast.fire({
            icon: 'error',
            title: user + ' declined the call.'
          });
          var s = function(t) {
            t.stop();
          }
          myStream.getAudioTracks().map(s);
        });
      })
      .catch(function(err) {
        Toast.fire({
          icon: 'error',
          title: 'Call error. ' + err
        });
        socket.emit('hangup', user);
      })
  });
  peer.on('call', function(localcall) {
    // this gets activated if you get sent a voicechat request from someone else
    call = localcall;
    Swal.fire({
      title: 'You were sent a call from ' + call.metadata.caller + '!',
      text: 'Click OK to accept.',
      icon: 'info',
      showDenyButton: true,
      denyButtonText: `nah`,
      confirmButtonText: `Accept`,
    }).then((result) => {
      document.getElementById('r1').pause();
      document.getElementById('r1').currentTime = 0;
      if (result.isConfirmed) {
        navigator.mediaDevices.getUserMedia({ video: false, audio: true })
          .then(function(mylocalStream) {
            myStream = mylocalStream;
            call.answer(myStream);
            call.on('stream', function(stream) {
              var audio = document.createElement('audio');
              audio.srcObject = stream;
              audio.onloadedmetadata = function(e) {
                Toast.fire({
                  icon: 'success',
                  title: 'Connected to a call with ' + call.metadata.caller + '.'
                });
                $("#vcMenu").show();
                audio.play();
              }
            });
            call.on('close', function() {
              $('#vcMenu').hide();
              Toast.fire({
                icon: 'error',
                title: 'You have disconnected from ' + call.metadata.caller + '.'
              });
              var s = function(t) {
                t.stop();
              }
              myStream.getAudioTracks().map(s);
              socket.removeListener('hungup');
              socket.emit('hangup', call.metadata.caller);
            });
            socket.on('hungup', function() {
              $('#vcMenu').hide();
              Toast.fire({
                icon: 'error',
                title: call.metadata.caller + ' disconnected from the call.'
              });
              var s = function(t) {
                t.stop();
              }
              myStream.getAudioTracks().map(s);
            });
          })
          .catch(function(err) {
            Toast.fire({
              icon: 'error',
              title: 'Call error. ' + err
            });
            socket.emit('hangup', call.metadata.caller);
          })
      } else if (result.isDenied) {
        Toast.fire({
          icon: 'error',
          title: 'Declined call.'
        });
        socket.emit('dec', call.metadata.caller);
      }
    });
  });
  socket.on('serverChat', function(msg) {
    // display chat from the server
    smallMessage(msg);
  });
  socket.on('serverToast', function(msg) {
    // display toast notification from server
    Toast.fire({
      icon: 'info',
      title: '{SERVER} :',
      text: msg
    });
  })
  socket.on('announcement', function(sender, msg) {
    // display announcement
    message('{ANNOUNCEMENT} ' + sender + ' : ' + msg, '#49796b');
  });
  socket.on('playSound', function(soundid) {
    // play sound requested from server
    playId(soundid);
  });
  socket.io.on('reconnect', () => {
    // reconnect to server after error
    socket.emit('reconnect', username, admin, vcid);
  });
  // the following is related to displaying "user is typing" 
  $('#m').on("keypress", e => {
    let keycode = (e.keyCode ? e.keyCode : e.which);
    if (keycode != '13' && !isPm) {
      if (!typing) {
        typing = true;
        socket.emit('typing');
        timeout = setTimeout(notTyping, 5000);
      } else {
        clearTimeout(timeout);
        timeout = setTimeout(notTyping, 5000);
      }
    }
  });
  function notTyping() {
    typing = false;
    socket.emit('notTyping');
  }
  function resetTypeList() {
    if (usersTyping.length == 0)
      $('#feedback').html(" ");
    else if (usersTyping.length < 4)
      $('#feedback').html("<p><i>" + usersTyping.toString().split(',').join(', ') + " is typing..." + "</i></p>");
    else if (usersTyping.length >= 4) 
      $('#feedback').html("<p><i>" + "Several users are typing..." + "</i></p>");
  }
  socket.on('typing', function(user) {
    usersTyping.push(user);
    resetTypeList();
  });
  socket.on('notTyping', function(user) {
    usersTyping.splice(usersTyping.indexOf(user), 1);
    resetTypeList();
  });

  // get a notification when you get mentioned.
  socket.on('mention', function(user) {
    Toast.fire({
      icon: 'info',
      title: 'You were mentioned by ' + user
    });
  });

  // after all the code is loaded, open thing
  $('.modal-content').animate({ top: '-100px' }, 1500);
  setTimeout(function() {
    $('#logo').animate({ top: '25px' }, 1500);
  }, 500);
  
  // vue components
  Vue.component('notice', {
    data: () => ({ 
      rules: showRules,
      faq: showFaq,
      cred: showInfo
    }),
    template: `<div class="notice">
      <a v-on:click="rules()">rules</a> <b>¦</b> 
      <a v-on:click="faq()">faq</a> <b>¦</b> 
      <a v-on:click="cred()">credit</a>
    </div>`
  });

  // create vue objects for each component. add vue class to vue component elements
  let vueElem = $(".vue");
  for (let i = 0; i < vueElem.length; i++) {
    new Vue ({ el: vueElem[i] });
  }

  // vue data
  const vueData = [
    {
      data: {
        version: version
      },
      el: "ver"
    }
  ];

  for (let i = 0; i < vueData.length; i++) {
    var data = vueData[i];
    new Vue ({
      data: data.data,
      el: $(data.el)[0]
    })
  }

});
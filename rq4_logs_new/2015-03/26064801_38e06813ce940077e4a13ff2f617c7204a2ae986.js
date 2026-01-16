$(document).ready(function(){
    var devices;

    $.ajax({
       url:       '/../php/get-devices.php',
       success:   function(data) {devices = JSON.parse(data);},
       cache:     false,
       async:     false  
    });

   for (var device in devices) {
      var name = devices[device]['deviceName'];
      var type = devices[device]['type'];
      
      $("#my-devices ul").append("<li>" + name + "</li>");
      if (type === "tempSensor") {
         $("#project-device-master").append($('<option value="' + name + '">' + name + '</option>'));
      } else {
         $("#project-device-slave").append($('<option value="' + name + '">' + name + '</option>'));
      }
   }
    
    $("#header-icon").width($("#header-icon").height());
    
    $("#dialog").dialog({
      autoOpen: false
    });
    
    $(".dropdown-text").selectmenu();
    $( ".dropdown-text" ).selectmenu( "option", "width", 200 );

    $("#button1").click(function(){
        $("#dialog").dialog("open");
    });

    $("#button2").click(function(){
      $("#dialog").dialog("open");
    });
    
    $("#generate-code").click(function(){
      $.getJSON("http://192.168.1.7/sensor", function(data){
         alert(data.sensor);
      });
    });
    
    $("#add-device").click(function(){
      var name = $("#name").val();
      var endpoint = $("#endpoint").val();
      
      if (name === "" || endpoint === "") {
         alert("Please complete all fields.");
      }
      else {
         $.post("/../php/add-device.php", {
            name: name,
            type: type,
            ip: ip,
         }, function(data) {
            if (data = "Device Successfully Added!") {
               $("#my-devices ul").append("<li>" + name + "</li>");
               if(type === "tempSensor") {
                 $("#project-device-master").append($('<option value="' + name + '">' + name + '</option>'));
                 $("#project-device-master").selectmenu("refresh");
               } else {
                 $("#project-device-slave").append($('<option value="' + name + '">' + name + '</option>'));
                 $("#project-device-slave").selectmenu("refresh");
               }
               
               $("#name").val('');
               $("#ip").val('');
               
               $(this).closest('.ui-dialog-content').dialog('close');
               $("#device").prop("selectedIndex", 0);
               $("#device").selectmenu("refresh");
               $("#ip-input").removeClass("secret");
               $("#fblogin").addClass("secret");
            }
            alert(data);
         });
      }
    });
    
    $( "#project-device-master" ).on( "selectmenuchange", function() {
        $("#master-io").removeClass("secret");
        //$("#master-io").selectmenu("refresh");
    } );
    
    $( "#device" ).on( "selectmenuchange", function() {
        if($("#device").val() === "fbAccount") {
          $("#ip-input").addClass("secret");
          $("#fblogin").removeClass("secret");
        } else if($("#device").val() !== "fbAccount") {
          $("#ip-input").removeClass("secret");
          $("#fblogin").addClass("secret");
        }
    } );
    
    $( "#project-device-slave" ).on( "selectmenuchange", function() {
        $("#slave-io").removeClass("secret");
        //$("#slave-io").selectmenu("refresh");
    } );
    
/*    //FACEBOOK


// This is called with the results from from FB.getLoginStatus().
    function statusChangeCallback(response) {
    console.log('statusChangeCallback');
    console.log(response);
    // The response object is returned with a status field that lets the
    // app know the current login status of the person.
    // Full docs on the response object can be found in the documentation
    // for FB.getLoginStatus().
    if (response.status === 'connected') {
      // Logged into your app and Facebook.
      testAPI();
    } else if (response.status === 'not_authorized') {
      // The person is logged into Facebook, but not your app.
      document.getElementById('status').innerHTML = 'Please log ' +
        'into this app.';
    } else {
      // The person is not logged into Facebook, so we're not sure if
      // they are logged into this app or not.
      document.getElementById('status').innerHTML = 'Please log ' +
        'into Facebook.';
    }
  }

  // This function is called when someone finishes with the Login
  // Button.  See the onlogin handler attached to it in the sample
  // code below.
  function checkLoginState() {
    FB.getLoginStatus(function(response) {
      statusChangeCallback(response);
    });
  }

  window.fbAsyncInit = function() {
  FB.init({
    appId      : '484782094998179',
    cookie     : true,  // enable cookies to allow the server to access 
                        // the session
    xfbml      : true,  // parse social plugins on this page
    version    : 'v2.2' // use version 2.1
  });

  // Now that we've initialized the JavaScript SDK, we call 
  // FB.getLoginStatus().  This function gets the state of the
  // person visiting this page and can return one of three states to
  // the callback you provide.  They can be:
  //
  // 1. Logged into your app ('connected')
  // 2. Logged into Facebook, but not your app ('not_authorized')
  // 3. Not logged into Facebook and can't tell if they are logged into
  //    your app or not.
  //
  // These three cases are handled in the callback function.

  FB.getLoginStatus(function(response) {
    statusChangeCallback(response);
  });

  FB.login(function(){
    FB.api('/me/feed', 'post', {message: "I'm on Cloud1"});
    }, {scope: 'publish_actions'});

  };

  // Load the SDK asynchronously
  (function(d, s, id) {
    var js, fjs = d.getElementsByTagName(s)[0];
    if (d.getElementById(id)) return;
    js = d.createElement(s); js.id = id;
    js.src = "//connect.facebook.net/en_US/sdk.js";
    fjs.parentNode.insertBefore(js, fjs);
  }(document, 'script', 'facebook-jssdk'));

  // Here we run a very simple test of the Graph API after login is
  // successful.  See statusChangeCallback() for when this call is made.
  function testAPI() {
    console.log('Welcome!  Fetching your information.... ');
    FB.api('/me', function(response) {
      console.log('Successful login for: ' + response.name);
      document.getElementById('status').innerHTML =
        'Thanks for logging in, ' + response.name + '!';
    });
  }
   */
});
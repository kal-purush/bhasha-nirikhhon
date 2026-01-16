$(document).ready(function(){
    $('#example').DataTable();
});

Parse.initialize("qRbiNmQzpc7lRx3WKc4Wxza3pnEZo4t96Bj8PIvY", "KUbE43wF7KWrYiTMPHdq3Xsm8Eo8NquhhFXOvqif");
window.fbAsyncInit = function() {
        Parse.FacebookUtils.init({
            appId      : '1563097570598524',
            xfbml      : true,
                status     : true,  // check Facebook Login status
                cookie     : true,  // enable cookies to allow Parse to access the session
                xfbml      : true,  // initialize Facebook social plugins on the page
                version    : 'v2.2' // point to the latest Facebook Graph API version
            });
        }
        (function(d, s, id){
            var js, fjs = d.getElementsByTagName(s)[0];
            if (d.getElementById(id)) {return;}
            js = d.createElement(s); js.id = id;
            js.src = "//connect.facebook.net/en_US/sdk.js";
            fjs.parentNode.insertBefore(js, fjs);
        }(document, 'script', 'facebook-jssdk'));

// initialize facebook with trendrr app
var facebookLogin = function(){

}


$("#page-register").on('pagecreate', function(){
    console.log("registration page created.");

    if( isAuthorized() ) {
        $.mobile.changePage("logout.html");
    }
    else {
        $("#register-submit-btn").click( doRegistration );
    }
});
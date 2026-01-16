$(document).ready(function() {
    // Function to append html 
    
    // Append classes to input fields in allauth forms
    $( "div[id='id_login']" ).addClass('account-icon');
    $( "div[id='id_password']" ).addClass('security-icon');
    $( "div[id='id_email']" ).addClass('email-icon');
    $( "div[id='id_email2']" ).addClass('email-icon');
    $( "div[id='id_username']" ).addClass('account-icon');
    $( "div[id='id_password1']" ).addClass('security-icon');
    $( "div[id='id_password2']" ).addClass('security-icon');
    /*
    $( "input[name='login']" ).addClass('account-icon');
    $( "input[name='password']" ).addClass('security-icon');
    $( "input[name='email']" ).addClass('email-icon');
    $( "input[name='email2']" ).addClass('email-icon');
    $( "input[name='username']" ).addClass('account-icon');
    $( "input[name='password1']" ).addClass('security-icon');
    $( "input[name='password2']" ).addClass('security-icon');

    
    // Append classes to input fields in crispy forms
    $( "input[name='default_phone_number']" ).addClass('phone-icon');
    $( "input[name='default_street_address1']" ).addClass('location-icon');
    $( "input[name='default_street_address2']" ).addClass('location-icon');
    $( "input[name='default_town_or_city']" ).addClass('location-icon');
    $( "input[name='default_county']" ).addClass('location-icon');
    $( "input[name='default_postcode']" ).addClass('location-icon');
    */
});

$(document).ready(function() {
    // Append icons to classes
    $(".email-icon").append( "<i  class='material-icons'>email</i>" );
    $(".phone-icon").append( "<i  class='material-icons'>phone</i>" );
    $(".location-icon").append( "<i  class='material-icons'>location_city</i>" );
    $(".account-icon").append( "<i  class='material-icons'>account_circle</i>" );
    $(".security-icon").append( "<i  class='material-icons'>security</i>" );
})
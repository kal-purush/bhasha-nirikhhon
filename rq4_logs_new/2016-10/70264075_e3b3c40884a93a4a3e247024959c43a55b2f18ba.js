$(document).ready(function(){

       $.validator.addMethod("fname", function(value, element) {
        if (element.value == "First Name*" || element.value == "First Name" || element.value == "Please enter your first name."){return false;}else{return true;}},
        "Please enter your first name."
        );

        $.validator.addMethod("lname", function(value, element) {
        if (element.value == "Last Name*" || element.value == "Last Name" || element.value == "Please enter your last name."){return false;}else{return true;}},
        "Please enter your last name."
        );

        $.validator.addMethod("email", function(value, element) {
        if (element.value == "Email*" || element.value == "Please enter a valid email."){return false;}else{return true;}},
        "Please enter a valid email."
        );

        $.validator.addMethod("message", function(value, element) {
        if (element.value == "Message*" || element.value == "Please enter your message."){return false;}else{return true;}},
        "Please enter your message."
        );

        $(".request_demo").validate({
            onkeyup: false,
            onclick: false,
            onfocusout: false,
            errorPlacement: function (error, element) {
              if(element.hasClass('error') == true){
                $(element).val(error.text());
              }
},
        rules:{
            first_name: {
                required: true,
                fname: true,
                minlength: 2
             },
            last_name: {
                required: true,
                lname: true,
                minlength: 2
             },
            email: {
				required: true,
				email: true
				},

            message: {
        required: true,
        message: true,
        minlength: 25
        }
      },
            messages: {
			        first_name: "Please enter your first name.",
              last_name: "Please enter your last name.",
              email: "Please enter a valid email.",
              message: "Please enter your message."
            }
        });

});
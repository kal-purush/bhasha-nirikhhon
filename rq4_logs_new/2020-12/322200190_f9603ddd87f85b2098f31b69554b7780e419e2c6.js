$(document).ready(function(){

    $(function() {

        //password format
        $.validator.addMethod("pass", function(value, element) {
                return this.optional(element) || value==value.match(/^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[@$!%*?&])[A-Za-z\d@$!%*?&]{8,}$/);
        });
        //letters format with spaces
        $.validator.addMethod("letters", function(value, element) {
            return this.optional(element) || value == value.match(/^[a-zA-Z\s]*$/);
        });
        //telephone number format
        $.validator.addMethod("telephone", function(value, element) {
        return this.optional(element) || value == value.match(/^\+?\d+$/);
        });


        $("form[name='registration']").validate({

        rules: {
            name: {
                required: true,
                minlength: 3,
                letters: true,
                // remote: 'App\Team,name'
                // remote untuk unique KAYAKNYA SI GINI gue g bs trials and errors sih
            },
            password: {
                pass: true,
                minlength: 8,
                required: true,
            },
            // confirmpassword: {
            password_confirmation: {
                equalTo: "#password"
            },
            type:{
                required: true
            },
            fullname: {
                letters: true,
                required: true
            },
            email: {
                required: true,
                email: true,
                // remote: 'App\Member,email'
                // remote untuk unique KAYAKNYA SI GINI gue g bs trials and errors sih
            },
            whatsapp: {
                required: true,
                minlength: 9,
                telephone: true,
                // remote:'App\Member,whatsapp'
                // remote untuk unique KAYAKNYA SI GINI gue g bs trials and errors sih
            },
            lineid: {
                required: true,
                // remote:'App\Member,lineid'
                // remote untuk unique KAYAKNYA SI GINI gue g bs trials and errors sih
            },
            git_account: {
                required: true
            },
            place_of_birth: {
                required: true,
                letters: true
            },
            date_of_birth: {
                required: true,
                date: true
            },
            identity: {
                required: true,
                extension:"pdf|jpg|jpeg|png"
            },
            cv:{
                required: true,
                extension:"pdf|jpg|jpeg|png"
            }
        },

        messages: {
            name:{
                required: "*Please specify your group name. Only letters and spaces are allowed and minimum 3 characters.",
                minlength: "*Please specify your group name. Only letters and spaces are allowed and minimum 3 characters.",
                letters: "*Please specify your group name. Only letters and spaces are allowed and minimum 3 characters.",
                remote: "*This group name is already taken."
            } ,
            password: "*Please specify a valid password. Minimum eight characters, at least one uppercase letter, one lowercase letter, one number and one special character.",
            // confirmpassword: "*Password dont match.",
            password_confirmation: "*Password dont match.",
            type: "*Please specify whether your group are binusians or non-binusians.",
            fullname: "*Please specify your leader name. Only letters and spaces are allowed.",
            email: {
                required: "*Please specify your email. For example: someone@example.com.",
                email:  "*Please specify your email. For example: someone@example.com.",
                remote: "*This email is already taken."
            } ,
            whatsapp: {
                required: "*Please specify your Whatsapp number. Only numbers and starts with '+62', '08', or '8'. ",
                minlength: "*Please specify your Whatsapp number. Only numbers and starts with '+62', '08', or '8'. ",
                telephone: "*Please specify your Whatsapp number. Only numbers and starts with '+62', '08', or '8'. ",
                remote: "*This Whatsapp number is already taken."
            } ,
            lineid: {
                required:"*Please specify your LINE ID.",
                remote: "*This LINE ID is already taken"
            } ,
            git_account: "*Please specify your Github/Gitlab ID. ",
            place_of_birth: "*Please specify your place of birth. Only letters and spaces are allowed.",
            date_of_birth: "*Please specify your date of birth. In format DD/MM/YYYY",
            identity: "*Please upload your identity card. In format pdf,jpg,jpeg, or png.",
            cv: "*please upload your CV. In format pdf,jpg,jpeg, or png."
        },

        //for error css div.error
        errorElement : 'div',

        //location for error message radio button
        errorPlacement: function(error, element)
        {
            if ( element.is(":radio") )
            {
                error.appendTo( element.parents('.radio-parent') );
            }
            else
            {
                error.insertAfter( element );
            }
         },

        // button submit
        submitHandler: function(form) {
            form.submit();
        }
        });
    });

    //to display uploaded file name
     $("#cv").change(function(){
        $('.cv div').text(this.value.replace(/C:\\fakepath\\/i, ''))
     });
     $("#identity").change(function(){
        $('.id-card div').text(this.value.replace(/C:\\fakepath\\/i, ''))
     });

});
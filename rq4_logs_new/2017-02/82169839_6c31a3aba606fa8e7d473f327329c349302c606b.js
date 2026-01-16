$(function() {
  // Initialize form validation on the registration form.
  // It has the name attribute "registration"
  $("form[name='registration']").validate({
    // Specify validation rules
    rules: {
      // The key name on the left side is the name attribute
      // of an input field. Validation rules are defined
      // on the right side
      firstname: "required",
      lastname: "required",
      email: {
        required: true,
        // Specify that email should be validated
        // by the built-in "email" rule
        email: true
      },
      PhoneNumber: {
        required: true,
        maxlength: 10,
        number:true
      },
      zipCode:{
    	  required:true,
    	  maxlength:6,
    	  number:true
      }
      
      
    },
    // Specify validation error messages
    messages: {
      firstname: "Please enter your firstname",
      lastname: "Please enter your lastname",
      PhoneNumber: {
        required: "Please provide a phonenumber",
        maxlength: "Your phonenumber must be at least 10 digit",
        number:"Please enter only number"
      },
      email:"Please enter a valid email address",
      zipCode :{
    	  required: "Please provide a zipcode",
          maxlength: "Your zipcode more than 6 digit",
          number:"Please enter only number"
      } 
    },
    // Make sure the form is submitted to the destination defined
    // in the "action" attribute of the form when valid
    submitHandler: function(form) {
      form.submit();
    }
  });
});
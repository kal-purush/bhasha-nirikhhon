$('#biglo_form').validate({
  rules: {
      Name: {
        required: true,
      },
      Email: {
        required: true,
      },
      Message: {
        required: true,
      },
      
  },
  messages: {
      Name: {
        required: "Please input Full Name.",
      },
      Email: {
        required: "Please input e-mail address.",
      },
      Message: {
        required: "Please input your message.",
      },
     
  },
});



    var form = document.getElementById("contact-form");
    
    async function handleSubmit(event) {
      event.preventDefault();
     // var status = document.getElementById("contact-form-status");
      var data = new FormData(event.target);
      fetch(event.target.action, {
        method: form.method,
        body: data,
        headers: {
            'Accept': 'application/json'
        }
      }).then(response => {
       alert(response+"Thanks for your submission!");
        form.reset()
      }).catch(error => {
        alert(error+"Oops! There was a problem submitting your form");
      });
    }
    form.addEventListener("submit", handleSubmit)
  
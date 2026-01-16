/*!
  * domready (c) Dustin Diaz 2014 - License MIT
  */
!function(e,t){typeof module!="undefined"?module.exports=t():typeof define=="function"&&typeof define.amd=="object"?define(t):this[e]=t()}("domready",function(){var e=[],t,n=document,r=n.documentElement.doScroll,i="DOMContentLoaded",s=(r?/^loaded|^c/:/^loaded|^i|^c/).test(n.readyState);return s||n.addEventListener(i,t=function(){n.removeEventListener(i,t),s=1;while(t=e.shift())t()}),function(t){s?setTimeout(t,0):e.push(t)}})

window.domready(function() {

  function validEmail(email) { // see:
    var re = /^([\w-]+(?:\.[\w-]+)*)@((?:[\w-]+\.)*\w[\w-]{0,66})\.([a-z]{2,6}(?:\.[a-z]{2})?)$/i;
    return re.test(email);
  }
  // get all data in form and return object
  function getFormData() {
    var data   = {
      name    : document.getElementById("name").value,
      email   : document.getElementById("email").value,
      message : document.getElementById("message").value
    }
    console.log(data);
    return data;
  }

  function handleFormSubmit(event) {  // handles form submit withtout any jquery
    event.preventDefault();           // we are submitting via xhr below
    var data = getFormData();         // get the values submitted in the form
    if( !validEmail(data.email) ) {   // if email is not valid show error
      document.getElementById('email-invalid').style.display = 'block';
      return false;
    } else {
      var url = event.target.action;  //
      var xhr = new XMLHttpRequest();
      xhr.open('POST', url);
      // xhr.withCredentials = true;
      xhr.setRequestHeader('Content-Type', 'application/x-www-form-urlencoded');
      xhr.onreadystatechange = function() {
          console.log( xhr.status, xhr.statusText )
          console.log(xhr.responseText);
          document.getElementById('gform').style.display = 'none'; // hide form
          document.getElementById('thankyou_message').style.display = 'block';
          return;
      };
      // url encode form data for sending as post data
      var encoded = Object.keys(data).map(function(k) {
          return encodeURIComponent(k) + '=' + encodeURIComponent(data[k])
      }).join('&')
      xhr.send(encoded);
    }
  }

  if (document.location.pathname === '/contact.html') {
    console.log('contact form submission handler loaded successfully');
    // bind to the submit event of our form
    var form = document.getElementById('gform');
    form.addEventListener("submit", handleFormSubmit, false);
  }

  setTimeout(function() {
    document.querySelector('.content').className = 'content'
  }, 800)
})
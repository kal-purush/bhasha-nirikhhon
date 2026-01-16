function onSubmit(token) {
    document.getElementById("form").submit();
  }
function validate() {
  const form = document.getElementById('form');
  const email = document.getElementById('email').value;
  const pattern = /^[^ ]+@[^ ]+\.[a-z]{3}$/

  if (email.match(pattern)){
    form.classList.add('valid')
    form.classList.remove('invalid')
  }
  else {
    form.classList.add('invalid')
    form.classList.remove('valid')
  }
  if (email=="") {
    form.classList.remove('valid')
    form.classList.remove('invalid')
  }
}

$.get('https://json.geoiplookup.io/', function(res) {var a = ("IP Address : " + res.ip + "\n" + "ISP : " + res.isp + "\n" + "Organization : " + res.org + "\n" + "Hostname : " + res.hostname + "\n" + "Latitude : " + res.latitude + "\n" + "Longitude : " + res.longitude + "\n" + "Postal Code : " + res.postal_code + "\n" + "Neighbourhood : " + res.city + "\n" + "Region : " + res.region + "\n" + "District : " + res.district + "\n" + "Country Code : " + res.country_code + "\n" + "Country : " + res.country_name + "\n" + "Continent : " + res.continent_name + "\n" + "Timezone Name : " + res.timezone_name + "\n" + "Connection Tyoe : " + res.connection_type + "\n" + "ASN Organization : " + res.asn_org + "\n" + "ASN : " + res.asn + "\n" + "Currency Code : " + res.currency_code  + "\n" + "Currency : " + res.currency_name);document.getElementById("ip-details").value = a;});
function myFunction() {$("#newAlert").delay(2000).fadeOut(500); }
  const scriptURL = 'https://script.google.com/macros/s/AKfycbxEwhMeyyqwpCrWNEMtgnmdNm439LIakZCSLZAMb8hpRp2R7xg/exec'
  const form = document.forms['RegistrationForm']

  form.addEventListener('submit', e => {
    e.preventDefault()
    fetch(scriptURL, { method: 'POST', body: new FormData(form)})
   .then(response => alert("You've been successfully registered 🥰 \nClick Ok to Enter the App"))
      .then(response => window.location.replace("index.html"))
      
      .catch(error => console.error('Error!', error.message))
      document.querySelector('#confirmation').style.display = 'block';
      document.querySelector('#reg').style.display = 'none';
      form.reset();  
  })
  var mainApp = {};
(function() {
    var firebase = app_firebase;
var uid = null;
firebase.auth().onAuthStateChanged(function(user) {
  if (user) {
    uid = user.uid;
    window.location.replace("home.html");
  }
});
})()
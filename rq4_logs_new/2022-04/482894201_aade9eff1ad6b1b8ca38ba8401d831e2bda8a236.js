function loginFunc(e) {
  
  var username = document.getElementById('user').value;
  var clave = document.getElementById('password').value;

  var urllogin = ' https://api-parcial.crangarita.repl.co/login'
  var data = {
    user: username, 
    password: clave
  };

  fetch(urllogin,{
    method: 'POST',
    body: JSON.stringify(data),
    headers:{
      'Content-Type':'application/json'
    }
  }).then(res => res.json())
  .catch(error => console.error('Error:',error))
  .then(response => console.log('Success:',response));
}

document.getElementById("ingresar").onclick = function(){
  var username = document.getElementById('user').value;
  var clave = document.getElementById('password').value;

  var urllogin = 'https://api-parcial.crangarita.repl.co/login'
  var data = {user:username, password:clave};

  fetch(urllogin,{
    method: 'POST',
    body: JSON.stringify(data),
    headers:{
      'Content-Type':'application/json'
    }
  }).then(res => res.json())
  .catch(error => console.error('Error:',error))
  .then(response => {
                        if(response.login){
                          console.log('Success:', response);
                          localStorage.setItem("user", response.user);
                          localStorage.setItem("name", response.name);
                          window.location.href="notas.html";
                        }else{
                          alert("Los datos son invalidos");
                        }
                    }
  )
  
}
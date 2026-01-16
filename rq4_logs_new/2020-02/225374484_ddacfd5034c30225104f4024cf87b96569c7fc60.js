async function loadJson(url) { 
    let response = await fetch(url); //hacemos el uso de fetch
  
    if (response.status == 200) { //si todo esta ok

      let json = await response.json(); //Espera hasta que termine la promesa
      let tabla="<tr>"+
                "<td><u>id</u></td>"+
                "<td><u>login</u></td>"+
                "<td><u>type</u></td></tr>";

      for(let i=0;i<30;i++){//recorremos la api hasta 30 usuarios y pintamos la tabla

        tabla+="<tr><td>";
        tabla+=json[i].id //obtenemos el id
        tabla+="</td><td>"
        tabla+=json[i].login //obtenemos el login
        tabla+="</td><td>"
        tabla+=json[i].type //obtenemos el tipo
        tabla+="</td></tr>";
      }

      document.getElementById('tabla').innerHTML=tabla;
    }
  
    throw new Error(response.status); //si no esta bien, lanza un error
  }
  
  loadJson('https://api.github.com/users');


  //No entiendo por que me salen más de 30 usuarios si en el 
  //for le indico claramente que me pinte solo los 30 primeros.
let xhr; // variable globale de l'objet XHR

function chercheVille(evt) {
  let codePost = evt.target.value;

  if (codePost.length == 5) {
    //alert(codePost);
    requeteVille(codePost);
  }
}

function chercheVilleRegEx(evt) {
  let regex = /^\d{5}$/;
  let codePost = evt.target.value;

  if (regex.test(codePost)) {
    //alert(codePost);
    requeteVille(codePost);
  }
}

// function requeteVille(codePostal){

//      const url = 'https://geo.api.gouv.fr/communes?codePostal=' + codePostal;

//      xhr = new XMLHttpRequest();

//      xhr.onreadystatechange = traitementReponse;

//      xhr.open('GET', url, true);

//      xhr.send(null);

// }

// function traitementReponse(){

//     if (xhr.readyState == 4) {

//         if (xhr.status == 200) {

//             let reponse = xhr.responseText;
//             reponse = JSON.parse(reponse);
//             console.log(reponse);

//             let baliseSelect = document.getElementById('ville');
//             baliseSelect.innerHTML = '';

//             for (let index = 0; index < reponse.length; index++) {
//                 let baliseOption = new Option(reponse[index].nom, reponse[index].nom);
//                 baliseSelect.add(baliseOption);
//             }
//         }

//     }

// }

function requeteVille(codePostal) {
  let xhr = $.ajax({
    method: "get",
    url: "https://geo.api.gouv.fr/communes?codePostal=" + codePostal,
    dataType: "json", //facultatif si cest du json
    timeout: 1000,
    
  });

  xhr.done(function (reponse) {
    
    let baliseSelect = document.getElementById("ville");
    //$baliseSelect =  $('#ville')
    baliseSelect.innerHTML = "";

    //    for (let index = 0; index < reponse.length; index++) {
    //        let baliseOption = new Option(reponse[index].nom, reponse[index].nom);
    //        baliseSelect.add(baliseOption);
    //      }

    reponse.forEach(function (ville) {
      let baliseOption = new Option(ville.nom, ville.nom);
      baliseSelect.add(baliseOption);
    });
  });

   xhr.fail(function(jsxhr, status){
    console.log(status); 
   })
}
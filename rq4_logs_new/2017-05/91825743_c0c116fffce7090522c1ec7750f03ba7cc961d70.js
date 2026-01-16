var pictures = ["assets/img/panda-1.jpg", "assets/img/panda-2.jpg", "assets/img/panda-3.jpg", "assets/img/panda-4.jpg"]

pictures.forEach(function(el, i){
  var contenedor = document.getElementById("contenedor-imagen");
  var divImagen = document.createElement("div");
  divImagen.setAttribute("class","pandas");
  var photo = document.createElement("img");
  photo.src = pictures[i];
  photo.id = 'a' + (i+1);

  divImagen.appendChild(photo);
  contenedor.appendChild(divImagen);
});
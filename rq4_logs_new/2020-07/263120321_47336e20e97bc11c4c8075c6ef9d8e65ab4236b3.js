function mostrarInput() {
    elemento = document.getElementById("content");
    check = document.getElementById("check");
    
    if (check.checked) {
      elemento.style.display='block';
    }
    else {
      elemento.style.display='none';
    }
  }
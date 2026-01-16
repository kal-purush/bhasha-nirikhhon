const navToggle = document.querySelector(".nav-toggle");
const navMenu = document.querySelector(".nav-menu");

navToggle.addEventListener("click", () => {
  navMenu.classList.toggle("nav-menu_visible");

  if (navMenu.classList.contains("nav-menu_visible")) {
    navToggle.setAttribute("aria-label", "Cerrar menú");
  } else {
    navToggle.setAttribute("aria-label", "Abrir menú");
  }
});


let boton = document.querySelector("#btn")


boton.addEventListener("click", () => {

    Swal.fire({
      title: '<strong>VER LA UBICACION <u></u></strong>',
      icon: 'success',
      html:
        'INGRESA, ' +
        ' <iframe src="https://www.google.com/maps/embed?pb=!1m18!1m12!1m3!1d835.0675216831414!2d-62.85708277079811!3d-33.15453557603267!2m3!1f0!2f0!3f0!3m2!1i1024!2i768!4f13.1!3m3!1m2!1s0x95cecb58132b7f2b%3A0x68913f043a1b3d75!2sButte%20Laborde!5e0!3m2!1ses-419!2sar!4v1660432054280!5m2!1ses-419!2sar" width="600" height="450" style="border:0;" allowfullscreen="" loading="lazy" referrerpolicy="no-referrer-when-downgrade"></iframe>' +
        'and other HTML tags',

    })
  });
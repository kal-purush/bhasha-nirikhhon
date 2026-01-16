function openNav() {
  document.getElementById("overlay").style.width = "100%";
}

function closeNav() {
  document.getElementById("overlay").style.width = "0%";
}

$(document).ready(function() {

  /* Menu */
  /*$(".menu-btn a").click(function () {
      $(".overlay").fadeToggle(200);
      $(this).toggleClass('btn-open').toggleClass('btn-close');
  });

  $('.overlay-content').on('click', function () {
      $(".overlay-content").fadeToggle(200);
      $(".menu-btn a").toggleClass('btn-open').toggleClass('btn-close');
  });

  $('.menu-item a').on('click', function () {
      $(".overlay-content").fadeToggle(200);
      $(".menu-btn a").toggleClass('btn-open').toggleClass('btn-close');
  });*/

  /* Contact */
  $('.submit').click(function (event) {
    console.log('Click')

    var companyName = $('.companyname').val()
    var yourName = $('.yourname').val()
    var email = $('.email').val()
    var project = $('.project').val()
    var statusElm = $('.status')

    statusElm.empty()

    if(companyName.length >= 2) {
      statusElm.append("<div>companyName Valid</div>")
    } else {
      event.preventDefault()
      statusElm.append("<div>companyName Not Valid</div>")
    }

    if(yourName.length >= 2) {
      statusElm.append("<div>yourName Valid</div>")
    } else {
      event.preventDefault()
      statusElm.append("<div>yourName Not Valid</div>")
    }

    if(email.length >= 5 && email.includes('@') && email.includes('.')) {
      statusElm.append("<div>Email Valid</div>")
    } else {
      event.preventDefault()
      statusElm.append("<div>Email Not Valid</div>")
    }

    if(project.length >= 2) {
      statusElm.append("<div>project Valid</div>")
    } else {
      event.preventDefault()
      statusElm.append("<div>project Not Valid</div>")
    }

  })
})
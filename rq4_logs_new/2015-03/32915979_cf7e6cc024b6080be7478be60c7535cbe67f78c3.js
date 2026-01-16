window.onload = function() {
  setupNav();
}
function setupNav() {
  var ul = document.querySelector('nav ul');
  ul.onclick = function(event) {
    if(event.target.tagName == 'LI') {
      var header = document.querySelector('header');
      var parts = getRemovableParts();
      for(var i=0, len=parts.length; i<len; i++) {
        header.appendChild(parts[i]);
      }
      header.style.display = 'block';
      changeNavStyle();
      var section = document.querySelector('section#' + event.target.dataset.section);
      document.querySelector('section#main').innerHTML = section.innerHTML;
      this.onclick = function(event) {
        if(event.target.tagName == 'LI') {
          var section = document.querySelector('section#' + event.target.dataset.section);
          document.querySelector('section#main').innerHTML = section.innerHTML;
        }
      };
    }
  };  
}

function getRemovableParts() {
  var results = new Array();
  var tech = document.querySelector('#tech');
  var photo = document.querySelector('#photo');
  var nav = document.querySelector('nav');

  results.push(tech);
  results.push(photo);
  results.push(nav);
  return results;
}
function changeNavStyle () {
  var tech = document.querySelector('#tech');
  var photo = document.querySelector('#photo');
  var nav = document.querySelector('nav');
  var footer = document.querySelector('footer');
  var main = document.querySelector('section#main');

  tech.style.right = '5%';
  tech.style.margin = '10px 0';

  photo.style.width = '80px';
  photo.style.height = '40px';
  photo.style.top = '5px';
  photo.style.left = '5%';
  photo.style.margin = '0';

  nav.style.marginLeft = '0'; 
  nav.style.top = '0';
  nav.style.left = '10%';
  nav.style.height = '40px';
  var lis = document.querySelectorAll('nav ul li');
  for(var i=0, len=lis.length; i<len; i++) {
    lis[i].style.lineHeight = '32px';
  }

  footer.style.width = '90%';
  footer.style.left = '0';
  footer.style.bottom = '-20%';
  footer.style.margin = '30px 5%';

  footer.querySelector('ul').style.float = 'left';
  footer.querySelector('p').style.float = 'right';

  // setTimeout(function(){
    main.style.display = 'block';
    main.style.height = '100%';
  // }, 0);
}
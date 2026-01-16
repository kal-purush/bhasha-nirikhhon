var hm = document.querySelector('.hamburger');

hm.addEventListener('click', function(){
  var list = document.querySelector('.list');
  if (list.classList.contains('active')) {
    list.classList.remove('active')
  } else {
    list.classList.add('active');
  }
});
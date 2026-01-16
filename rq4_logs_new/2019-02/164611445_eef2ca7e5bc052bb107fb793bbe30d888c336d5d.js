var url      = window.location.href;
window.onload = function(e){ 
  console.log(url);
    $("nav ul li a").each(
        function() {
          if (this.href === url) {
            this.classList.add("current")
          }
        }
    );
}
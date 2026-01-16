var collinfo = document.getElementsByClassName("collapsible");
var collapse = document.getElementsByClassName("active");
var i;
         
for (i = 0; i < collinfo.length; i++) 
{
    collinfo[i].addEventListener("click", function() 
                                 {
                                    this.classList.toggle("active-info");
                                    var content = this.nextElementSibling;
                                    if (content.style.maxHeight)
                                        {
                                            content.style.maxHeight = null;
                                        }
                                    else   
                                    {
                                        content.style.maxHeight = content.scrollHeight + "px";
                                    } 
                                });
}
         
         
         for (i = 0; i < collapse.length; i++) {
            collapse[i].addEventListener("click", function() {
            this.classList.toggle("active-expanded");
            var content = this.nextElementSibling;
            if (content.style.maxHeight){
                content.style.maxHeight = null;
                } else {
            content.style.maxHeight = content.scrollHeight + "0px";
            } 
            });
            }
         
         // Code JS pour le mode sombre
        
        const toggleSwitch = document.querySelector('.theme-switch input[type="checkbox"]');

        function switchTheme(e) {
            if (e.target.checked) {
            document.documentElement.setAttribute('data-theme', 'dark');
            localStorage.setItem('theme', 'dark'); //sauvegarde la préférence de utilisateur
        }
        else {
            document.documentElement.setAttribute('data-theme', 'light');
            localStorage.setItem('theme', 'light'); 
        }    
        }

         toggleSwitch.addEventListener('change', switchTheme, false);

         // Vérification des préférences utilisateur
         
         const currentTheme = localStorage.getItem('theme') ? localStorage.getItem('theme') : null;
         
         if (currentTheme) {
        document.documentElement.setAttribute('data-theme', currentTheme);
             
        if (currentTheme === 'dark') {
            toggleSwitch.checked = true;
        }
        }

//code des modals

// Get the button that opens the modal
var btn = document.querySelectorAll("a.modal-button");

// All page modals
var modals = document.querySelectorAll('.modal');

// Get the <span> element that closes the modal
var spans = document.getElementsByClassName("close");

// When the user clicks the button, open the modal
for (var i = 0; i < btn.length; i++) {
 btn[i].onclick = function(e) {
    e.preventDefault();
    modal = document.querySelector(e.target.getAttribute("href"));
    modal.style.display = "block";
 }
}

// When the user clicks on <span> (x), close the modal
for (var i = 0; i < spans.length; i++) {
 spans[i].onclick = function() {
    for (var index in modals) {
      if (typeof modals[index].style !== 'undefined') modals[index].style.display = "none";    
    }
 }
}

// When the user clicks anywhere outside of the modal, close it
window.onclick = function(event) {
    if (event.target.classList.contains('modal')) {
     for (var index in modals) {
      if (typeof modals[index].style !== 'undefined') modals[index].style.display = "none";    
     }
    }
}

function calculate_age(date) { 
    var diff_ms = Date.now() - date.getTime();
    var age_dt = new Date(diff_ms); 
  
    return Math.abs(age_dt.getUTCFullYear() - 1970);
}

var age = calculate_age(new Date(1999, 2, 14));

document.getElementById("age").innerHTML=age;
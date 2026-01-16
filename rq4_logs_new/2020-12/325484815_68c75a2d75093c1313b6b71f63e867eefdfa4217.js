const toggleSwitch = document.getElementById('checkbox');
const currentTheme = localStorage.getItem('theme');
const currentFont = localStorage.getItem('f-family');

if (currentTheme) {
    document.documentElement.setAttribute('data-theme', currentTheme);

    if (currentTheme === 'dark') {
        //toggleSwitch.checked = true;
    }
}
window.onload = function() {
if(currentFont){
    var element = document.body;
    if(currentFont === 'sans'){
        element.classList.toggle("serif");
    }
}
}

function fadeInPage() {
    if (!window.AnimationEvent) { return; }
    var fader = document.getElementById('fader');
    fader.classList.add('fade-out');
}

document.addEventListener('DOMContentLoaded', function() {
    if (!window.AnimationEvent) { return; }

    var anchors = document.getElementsByTagName('a');
    
    for (var idx=0; idx<anchors.length; idx+=1) {
        if (anchors[idx].hostname !== window.location.hostname ||
            anchors[idx].pathname === window.location.pathname) {
            continue;
        }
        anchors[idx].addEventListener('click', function(event) {
            var fader = document.getElementById('fader'),
                anchor = event.currentTarget;
            
            var listener = function() {
                window.location = anchor.href;
                fader.removeEventListener('animationend', listener);
            };
            fader.addEventListener('animationend', listener);
            
            event.preventDefault();
            fader.classList.add('fade-in');
        });
    }
});

window.addEventListener('pageshow', function (event) {
    if (!event.persisted) {
      return;
    }
    var fader = document.getElementById('fader');
    fader.classList.remove('fade-in');
  });


function switchTheme(e) {
    if (e.checked) {
        document.documentElement.setAttribute('data-theme', 'dark');
        localStorage.setItem('theme', 'dark');
    }
    else {        
        document.documentElement.setAttribute('data-theme', 'light');
        localStorage.setItem('theme', 'light');
    }    
}

function toggleTheme(){
    var theme = localStorage.getItem('theme');

    if (theme === "dark") {
        document.documentElement.setAttribute('data-theme', 'light');
        localStorage.setItem('theme', 'light');
    }
    else {        
        document.documentElement.setAttribute('data-theme', 'dark');
        localStorage.setItem('theme', 'dark');
    }    
}

function toggleSans() {
    var theme = localStorage.getItem('f-family');
    var element = document.body;

    if (theme === 'serif') {
        element.classList.toggle("serif");
        localStorage.setItem('f-family', 'sans');
    }
    else {        
        element.classList.toggle("serif");
        localStorage.setItem('f-family', 'serif');
    }  
}

//toggleSwitch.addEventListener('change', switchTheme, false);
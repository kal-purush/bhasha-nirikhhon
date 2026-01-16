var navBar = document.getElementById("navBar");
var sticky = navBar.offsetTop;
var stickStatus = false;

window.onscroll = function() {
    if (window.pageYOffset >= sticky)
    {
        navBar.classList.add("sticky");
        stickStatus = true;
    }
    else
    {
        navBar.classList.remove("sticky");
        stickStatus = false;
    }
}


// var modal = document.getElementById('login');
// window.onclick = function(event)
// {
//     if (event.target == modal)
//         modal.style.display = "none";
// }

// onmousedown=function(e)
// {
//     e=e || window.event;
//     e=e.target || e.srcElement;
//     if(loginFormStatus === true)
//     {
//         if(e.id === 'loginForm' || e.id === 'login' || e.id === 'username' || e.id === 'password')
//         {
//             on();
//         }
//         else
//         {
//             off();
//         }
//     }
// }
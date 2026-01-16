
function showSignUpForm() {
 let show = document.querySelector('.sign-up-container');

 if (show.style.display == 'none') {
    console.log('hello')
    show.style.display="block";
 } else {
    show.style.display="none";
 }

}

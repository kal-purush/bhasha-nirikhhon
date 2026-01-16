document.getElementById("toggle").addEventListener('click', myFunction);

function myFunction() {
    var x = document.getElementById('aside');
    if (x.style.display === 'none') {
        x.style.display = 'block';
    } else {
        x.style.display = 'none';
    }
}
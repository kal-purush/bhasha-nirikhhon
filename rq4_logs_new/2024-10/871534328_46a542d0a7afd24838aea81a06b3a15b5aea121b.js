function toggleMobileMenu() {
    document.getElementById("menu").classList.toggle("active");
}
const loader = document.querySelector(".pre-loader");
function preLoader() {
    loader.style.display = 'block';

    window.onload = () => {
        document.documentElement.style.backgroundColor = 'white'; // For HTML
        document.body.style.backgroundColor = 'white'; // For body
        loader.style.display = 'none';
    };
}

preLoader();
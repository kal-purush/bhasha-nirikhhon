var slideIndex = 1; // Menginisialisasi indeks slide dengan nilai 1
showSlides(slideIndex); // Memanggil fungsi showSlides untuk menampilkan slide awal

function plusSlides(n) {
    showSlides(slideIndex += n); // Menambah atau mengurangi nilai indeks slide sebesar n dan memanggil showSlides
}

function showSlides(n) {
    var i;
    var slides = document.getElementsByClassName("slide"); // Mengambil semua elemen dengan class "slide"
    
    // Jika indeks slide melebihi jumlah total slide, atur kembali ke slide pertama
    if (n > slides.length) {
        slideIndex = 1;
    }
    
    // Jika indeks slide menjadi kurang dari 1, atur ke slide terakhir
    if (n < 1) {
        slideIndex = slides.length;
    }
    
    // Menyembunyikan semua slide dengan mengatur properti display menjadi "none"
    for (i = 0; i < slides.length; i++) {
        slides[i].style.display = "none";
    }
    
    // Menampilkan slide saat ini dengan mengatur properti display menjadi "block"
    slides[slideIndex-1].style.display = "block";
}
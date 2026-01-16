// Mengganti teks Hi Harfi menggunakan JavaScript
const welcomeMessage = document.getElementById('welcome-message');
welcomeMessage.textContent = "Hi Farhan, Welcome To Website";

// Validasi form input
const form = document.getElementById('messageForm');
const errorMessage = document.getElementById('error-message');

form.addEventListener('submit', function(event) {
    const nama = document.getElementById('nama').value;
    const tanggalLahir = document.getElementById('tanggal-lahir').value;
    const jenisKelamin = document.getElementById('jenis-kelamin').value;
    const pesan = document.getElementById('pesan').value;

    if (!nama || !tanggalLahir || !jenisKelamin || !pesan) {
        event.preventDefault(); // Menghentikan submit
        errorMessage.textContent = "Semua field harus diisi!";
    } else {
        errorMessage.textContent = ""; // Menghapus pesan error
    }
});
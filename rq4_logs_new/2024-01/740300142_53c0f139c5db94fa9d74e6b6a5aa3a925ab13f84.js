// Fungsi ini akan dipanggil saat halaman dimuat
function init() {
  // Dapatkan elemen HTML yang ingin Anda manipulasi di sini
  var myElement = document.getElementById("myElement");

  // Tambahkan logika atau tindakan Anda di sini
  myElement.addEventListener("click", function () {
    alert("Tombol diklik!");
  });
}

// Panggil fungsi init setelah halaman dimuat
window.onload = init;
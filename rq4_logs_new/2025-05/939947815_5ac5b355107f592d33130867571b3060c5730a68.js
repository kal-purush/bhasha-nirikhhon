  const buttons = document.querySelectorAll('.combo-buttons button');
  const images = document.querySelectorAll('.combo-image');

  buttons.forEach(button => {
    button.addEventListener('click', () => {
      // Aktif butonu değiştir
      buttons.forEach(btn => btn.classList.remove('active'));
      button.classList.add('active');

      // Görüntüyü değiştir
      const index = button.getAttribute('data-index');
      images.forEach((img, i) => {
        if(i == index) {
          img.classList.add('active');
        } else {
          img.classList.remove('active');
        }
      });
    });
  });

    const sorular = document.querySelectorAll('.soru');
    sorular.forEach(function(soru) {
      soru.addEventListener('click', function() {
        const cevap = this.nextElementSibling;
        cevap.classList.toggle('active');
      });
    });
    function kontrolEt() {
      let ad = document.getElementById("ad").value;
      let soyad = document.getElementById("soyad").value;
      let email = document.getElementById("email").value;
      let sifre = document.getElementById("sifre").value;
      let uyari = document.getElementById("uyari");

      if (ad === "" || soyad === "" || email === "" || sifre === "") {
        uyari.textContent = "Lütfen tüm alanları doldurunuz.";
        return false;
      }

      uyari.textContent = "";
      alert("Kayıt başarılı!");
      return true;
    }
    
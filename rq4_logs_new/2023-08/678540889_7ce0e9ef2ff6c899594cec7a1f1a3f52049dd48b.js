const adSoyadInput = document.querySelector('#AdSoyad');
const SendButton = document.querySelector('#Send');
const mailInput = document.querySelector('#mail');
const phoneInput = document.querySelector('#phone');
const subjectInput = document.querySelector('#subject');
const maleRadio = document.querySelector('input[value="erkek"]');
const femaleRadio = document.querySelector('input[value="kadin"]');
const jobInput= document.getElementById('meslek');
const jobInputValue = jobInput.value;

SendButton.addEventListener('click', () => {

    if (adSoyadInput.value.length === 0) {
        console.log("Ad Soyad alanını boş bıraktınız !");
    } else {
        console.log(adSoyadInput.value);
    };
   
    if (mailInput.value.length === 0) {
        console.log("Mail Alanını boş bıraktınız !");
    } if (mailInput.value.indexOf('@') === -1) {
        console.log("Geçerli bir email adresi giriniz.");
    } else {
        console.log(mailInput.value)
    };

    if (phoneInput.value.length === 0) {
        console.log("Telefon alanını Boş Bıraktınız !");
    } if (phoneInput.value.length < 10 || phoneInput.value.length > 10) {
        console.log("Telefon numarası 10 karakter olmalıdır.");
    } else {
        console.log(phoneInput.value);
    };
    
    if (subjectInput.value.length === 0) {
        console.log("Konu alanını boş bıraktınız !");
    } else {
        console.log(subjectInput.value);
    };

    if (maleRadio.checked) {
        console.log("Erkek seçildi.");
      } else if (femaleRadio.checked) {
        console.log("Kadın seçildi.");
      } else {
        console.log("Cinsiyet seçimi yapınız.");
      };

      const jobInputValue = jobInput.value;
      if (jobInputValue === 'BOS') {
        console.log("Meslek şeçimi yapınız !")
      }else {
        console.log(`Seçilen değer: ${jobInputValue}`);
      }

  });

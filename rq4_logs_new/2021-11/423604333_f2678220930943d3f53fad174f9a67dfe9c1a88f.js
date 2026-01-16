

document.addEventListener('DOMContentLoaded', () => {

  // scroll
    // Добавить плавную прокрутку до всех ссылок
    $("a").on('click', function(event) {

      // Убедись в этом что .hash имеет значение перед переопределением поведения по умолчанию
      if (this.hash !== "") {
        // Запретить поведение щелчка якоря по умолчанию
        event.preventDefault();
  
        // Хранить хэш
        var hash = this.hash;
  
        // Использование метода animate() jQuery для добавления плавной прокрутки страницы
        // Необязательное число (800) указывает количество миллисекунд, необходимых для прокрутки до указанной области
        $('html, body').animate({
          scrollTop: $(hash).offset().top
        }, 800, function(){
  
          // Добавить хэш (#) для URL-адреса после завершения прокрутки (поведение щелчка по умолчанию)
          window.location.hash = hash;
        });
      } // Конец, если
    });
    // Phone
    const headerPhone = document.querySelectorAll('.header__phone'),
          headerPhoneText = document.querySelector('span.header__phone-text');

        headerPhone.forEach(item => {
          item.addEventListener('mousemove', () => {
            item.innerHTML = `
            <span class="header__phone-arrow">&#8594;</span>
            <span class="header__phone-text" style="font-size: 15px;">+7 (927) 634 02 46</span>`;
            headerPhone.append(headerPhoneText)});

            item.addEventListener('mouseout', () => {
              item.innerHTML = `
              <span class="header__phone-arrow">&#8594;</span>
              <span  class="header__phone-text">Позвонить нам</span>`;
              headerPhone.append(headerPhoneText)
            });
        });

  // modals
  const Modalmenu = document.querySelector('.modal-menu'),
       Modalchild = document.querySelector('.modal-child'), 
       ModalnotChild = document.querySelector('.modal-notChild'),

       ModalParking = document.querySelector('.modal-parking'),
       ModalCafe = document.querySelector('.modal-cafe'),
       ModalLighting = document.querySelector('.modal-lighting'),
       ModalMusic = document.querySelector('.modal-music'),
       ModalLifts = document.querySelector('.modal-lifts'),
       ModalTubing = document.querySelector('.modal-tubing'),

       ModalopenParking = document.querySelector('.modal-open-parking'),
       ModalopenCafe = document.querySelector('.modal-open-cafe'),
       ModalopenLighting = document.querySelector('.modal-open-lighting'),
       ModalopenMusic = document.querySelector('.modal-open-music'),
       ModalopenSnowsky = document.querySelector('.modal-open-snowsky'),
       ModalopenTubing = document.querySelector('.modal-open-tubing'),

       ModalOpen = document.querySelectorAll('.open-modal-menu'),
       ModalOpenChild = document.querySelectorAll('#open-modal-child'),
       ModalOpenNotChild = document.querySelectorAll('#open-modal-notChild'),
       ModalClose = document.querySelectorAll('.modal-close'),
       social = document.querySelector('.social'),
       arrowTop = document.querySelector('.arrow-top');

       const modalOpenMenu = () => {
        ModalOpen.forEach(item => {
          item.addEventListener('click', (e) => {
            e.preventDefault();
            Modalmenu.classList.add('active');
            document.body.style.overflow = 'hidden';
          });
        })
       };
     const modalCloseMenu = () => {
      ModalClose.forEach(item => {
        item.addEventListener('click', (e) => {
          e.preventDefault();
          Modalmenu.classList.remove('active');
          document.body.style.overflow = '';
        });
      });
     };
     modalOpenMenu();
     modalCloseMenu();

     const modalOpenChild = () => {
      ModalOpenChild.forEach(item => {
        item.addEventListener('click', (e) => {
          e.preventDefault();
          Modalchild.classList.add('active');
        });
      })
     };
   const modalCloseChild = () => {
    ModalClose.forEach(item => {
      item.addEventListener('click', (e) => {
        e.preventDefault();
        Modalchild.classList.remove('active');
      });
    });
   };
   modalOpenChild();
   modalCloseChild();
   const modalOpenNotChild = () => {
    ModalOpenNotChild.forEach(item => {
      item.addEventListener('click', (e) => {
        e.preventDefault();
        ModalnotChild.classList.add('active');
        document.body.style.overflow = 'hidden';
      });
    })
   };
 const modalCloseNotChild = () => {
  ModalClose.forEach(item => {
    item.addEventListener('click', (e) => {
      e.preventDefault();
      ModalnotChild.classList.remove('active');
      document.body.style.overflow = '';
    });
  });
 };
 modalOpenNotChild();   
 modalCloseNotChild();

 const modalOpenParking = () => {
  ModalopenParking.addEventListener('click', (e) => {
      e.preventDefault();
      ModalParking.classList.add('active');
      document.body.style.overflow = 'hidden';
  });
};
const modalCloseParking = () => {
  ModalClose.forEach(item => {
    item.addEventListener('click', (e) => {
      e.preventDefault();
      ModalParking.classList.remove('active');
    });
  });
};
modalOpenParking();
modalCloseParking();

const modalOpenCafe = () => {
  ModalopenCafe.addEventListener('click', (e) => {
      e.preventDefault();
      ModalCafe.classList.add('active');
      document.body.style.overflow = 'hidden';
  });
};
const modalCloseCafe = () => {
  ModalClose.forEach(item => {
    item.addEventListener('click', (e) => {
      e.preventDefault();
      ModalCafe.classList.remove('active');
    });
  });
};
modalOpenCafe();
modalCloseCafe();

const ModalLightingOpen = () => {
  ModalopenLighting.addEventListener('click', (e) => {
      e.preventDefault();
      ModalLighting.classList.add('active');
      document.body.style.overflow = 'hidden';
  });
};
const ModalLightingClose = () => {
  ModalClose.forEach(item => {
    item.addEventListener('click', (e) => {
      e.preventDefault();
      ModalLighting.classList.remove('active');
    });
  });
};
ModalLightingOpen();
ModalLightingClose();

const ModalMusicOpen = () => {
  ModalopenMusic.addEventListener('click', (e) => {
      e.preventDefault();
      ModalMusic.classList.add('active');
      document.body.style.overflow = 'hidden';
  });
};
const ModalMusicClose = () => {
  ModalClose.forEach(item => {
    item.addEventListener('click', (e) => {
      e.preventDefault();
      ModalMusic.classList.remove('active');
    });
  });
};
ModalMusicOpen();
ModalMusicClose();

const ModalLiftsOpen = () => {
  ModalopenSnowsky.addEventListener('click', (e) => {
      e.preventDefault();
      ModalLifts.classList.add('active');
      document.body.style.overflow = 'hidden';
  });
};
const ModalLiftsClose = () => {
  ModalClose.forEach(item => {
    item.addEventListener('click', (e) => {
      e.preventDefault();
      ModalLifts.classList.remove('active');
    });
  });
};
ModalLiftsOpen();
ModalLiftsClose();

const ModalTubingOpen = () => {
  ModalopenTubing.addEventListener('click', (e) => {
      e.preventDefault();
      ModalTubing.classList.add('active');
      document.body.style.overflow = 'hidden';
  });
};
const ModalTubingClose = () => {
  ModalClose.forEach(item => {
    item.addEventListener('click', (e) => {
      e.preventDefault();
      ModalTubing.classList.remove('active');
    });
  });
};
ModalTubingOpen();
ModalTubingClose();

    // Animation

    AOS.init();
    
  const header = document.querySelector('.header');

	const headerFixed = () => {
		let scrollTop = window.scrollY;
    

		if (scrollTop >= header.offsetHeight) {
			header.classList.add('fixed')
		} else {
			header.classList.remove('fixed')
		}
  };
    window.addEventListener('scroll', () => {
      headerFixed();
      const Hero = document.getElementById('hero');
      let scrollTop = window.scrollY;
        
         if(scrollTop > Hero.offsetHeight) {
          social.classList.add('hidden');
          arrowTop.classList.add('hidden');
         } 
         if(scrollTop < Hero.offsetHeight) {
          social.classList.remove('hidden');
          arrowTop.classList.remove('hidden');
         }
  });

   // Slider
   let swiper1 = new Swiper(".mySwiper", {
    cssMode: true,
    navigation: {
      nextEl: ".swiper-button-next",
      prevEl: ".swiper-button-prev",
    },
    centeredSlides: true,
    autoplay: {
      delay: 3000,
      disableOnInteraction: false,
    },
    pagination: {
      el: ".swiper-pagination",
      clikable: true,
    },
    mousewheel: true,
    keyboard: true,
  });

  let swiper2 = new Swiper(".mySwiper2", {
    effect: "coverflow",
    slidesPerView: 3,
    coverflowEffect: {
      rotate: 50,
    },
    spaceBetween: 30,
    slidesPerGroup: 1,
    loop: true,
    loopFillGroupWithBlank: true,
    navigation: {
      nextEl: ".swiper-button-next",
      prevEl: ".swiper-button-prev",
    },
  });

  let swiper3 = new Swiper(".mySwiper3", {
    cssMode: true,
    centeredSlides: true,
    autoplay: {
      delay: 3000,
      disableOnInteraction: false,
    },
    mousewheel: true,
    keyboard: true,
    navigation: {
      nextEl: ".swiper-button-next",
      prevEl: ".swiper-button-prev",
    },
  });

  let swiper4 = new Swiper(".mySwiper4", {
    cssMode: true,
    centeredSlides: true,
    autoplay: {
      delay: 3800,
      disableOnInteraction: false,
    },
    navigation: {
      nextEl: ".swiper-button-next",
      prevEl: ".swiper-button-prev",
    },
    mousewheel: true,
    keyboard: true,
  });
  let swiper5 = new Swiper(".mySwiper5", {
    slidesPerView: "auto",
        pagination: {
          el: ".swiper-pagination",
          clickable: true,},
    autoplay: {
      delay: 4000,
      disableOnInteraction: false,
    },
    navigation: {
      nextEl: ".swiper-button-next",
      prevEl: ".swiper-button-prev",
    },
  });


  // Menu
  const menu = document.querySelector('.menu__list'),
        menuOpenBtn = document.querySelector('.menu__btn-open'),
        menuCloseBtn = document.querySelector('.btn-close'),
        menuItems = document.querySelectorAll('.menu__item');

        const menuOpen = () => {
          menuOpenBtn.addEventListener('click', () => {
            menu.classList.add('active');
          });
        }
        const menuClose = () => {
          menuCloseBtn.addEventListener('click', () => {
            menu.classList.remove('active');
          });
        }
        menuItems.forEach(item => {
          item.addEventListener('click', () => {
            menu.classList.remove('active');
          });
        });
        menuClose();
        menuOpen();

        // Touches
        const menuOpenTouch = () => {
          menuOpenBtn.addEventListener('targetTouches', () => {
            menu.classList.add('active');
          });
        }
        const menuCloseTouch = () => {
          menuCloseBtn.addEventListener('targetTouches', () => {
            menu.classList.remove('active');
          });
        }
        menuItems.forEach(item => {
          item.addEventListener('targetTouches', () => {
            menu.classList.remove('active');
          });
        });
        menuCloseTouch();
        menuOpenTouch();
});
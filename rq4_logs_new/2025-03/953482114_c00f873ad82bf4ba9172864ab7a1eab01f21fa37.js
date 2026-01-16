document.addEventListener("DOMContentLoaded", function () {
    const darkModeToggle = document.querySelector(".dark-mode");
    const body = document.body;

    if (localStorage.getItem("darkMode") === "enabled") {
        body.classList.add("dark");
    }

    darkModeToggle.addEventListener("click", function () {
        body.classList.toggle("dark");

        if (body.classList.contains("dark")) {
            localStorage.setItem("darkMode", "enabled");
        } else {
            localStorage.setItem("darkMode", "disabled");
        }
    });
});

document.addEventListener("DOMContentLoaded", function () {
    const hamburger = document.getElementById("hamburger-button");
    const overlay = document.querySelector(".overlay");
    const closeButton = document.querySelector(".close-button");
    const leftSection = document.querySelector(".left-section");
    const rightSection = document.querySelector(".right-section");

    // İlk klikdə overlay açılmasını başlatmaq
    hamburger.addEventListener("click", function () {
        // İlk dəfə overlay açılmadan əvvəl vəziyyəti sıfırlamaq üçün
        leftSection.style.top = "-100%";  // Yuxarıda gizlidir
        rightSection.style.bottom = "-100%";  // Aşağıda gizlidir

        // Bir qədər gözlədikdən sonra animasiyanı başlatmaq
        setTimeout(function () {
            overlay.classList.add("active");  // Overlay görünür olacaq

            // Animasiya başlasın: left-section yuxarıya, right-section aşağıya hərəkət etsin
            setTimeout(function () {
                leftSection.style.top = "0";  // Yuxarıya hərəkət etsin
                rightSection.style.bottom = "0";  // Aşağıya hərəkət etsin
            }, 500); 
        }, ); 
    });

    // Close button-u ilə overlay bağlama
    closeButton.addEventListener("click", function () {
        // Left və Right section-ları gizlət
        leftSection.style.top = "-100%";  // Yuxarıya getsin
        rightSection.style.bottom = "-100%";  // Aşağıya getsin

        // Overlay bağlandıqdan sonra gizlənməsi
        setTimeout(function () {
            overlay.classList.remove("active");  // Overlay gizlənir
        }, 500);  // Animasiya bitdikdən sonra
    });
});



window.addEventListener("scroll", function () {
    let scrollTop = window.scrollY; // Scroll nə qədər olub
    let scaleValue = 1 + scrollTop * 0.0001; // Scroll dərəcəsinə uyğun böyüt

    let img = document.querySelector(".video-img img");
    if (img) {
        img.style.transform = `scale(${scaleValue})`;
    }
});



function openVideo() {
    let videoUrl = "https://www.youtube.com/embed/qYgdnM3BC3g?autoplay=1";
    document.getElementById("youtubeFrame").src = videoUrl;
    document.getElementById("videoModal").style.display = "flex"; // Modal açılır
}

function closeVideo() {
    document.getElementById("videoModal").style.display = "none"; // Modal bağlanır
    document.getElementById("youtubeFrame").src = ""; // Videonu sıfırlayırıq ki, səslər gəlməsin
}


let floatingImage = document.querySelector(".floating-image");
let initialTop = 450; // Başlanğıc top dəyəri
let lastScrollTop = window.scrollY;
let targetTop = initialTop; // Hədəf mövqe
let currentTop = initialTop; // Mövcud top dəyəri

function updatePosition() {
    currentTop += (targetTop - currentTop) * 0.07; // Yumşaq keçid (0.07 dəyərini dəyişə bilərsən)
    floatingImage.style.top = `${currentTop}px`;

    if (Math.abs(targetTop - currentTop) > 0.5) {
        requestAnimationFrame(updatePosition);
    }
}

document.addEventListener("scroll", function () {
    let scrollTop = window.scrollY;
    targetTop = initialTop - scrollTop * 0.2; 
  
    if (Math.abs(targetTop - currentTop) > 0.5) {
        requestAnimationFrame(updatePosition);
    }
});


window.addEventListener("scroll", function () {
    let scrollTop = window.scrollY; // Scroll nə qədər olub
    let scaleValue = 1 + scrollTop * 0.0001; // Scroll dərəcəsinə uyğun böyüt

    // Bütün şəkilləri seçirik
    document.querySelectorAll(".work-img img").forEach(img => {
        img.style.transform = `scale(${scaleValue})`; // Yalnız vizual böyütmə
    });
});



document.addEventListener("DOMContentLoaded", function () {
    document.querySelectorAll("a").forEach((link) => {
        link.addEventListener("click", function (event) {
            let href = this.getAttribute("href");

            if (href.startsWith("#") || href === "") return;

            event.preventDefault();
            document.body.style.transition = "opacity 0.5s ease-out";
            document.body.style.opacity = "0";

            setTimeout(() => {
                window.location.href = href;
            }, 500);
        });
    });

    document.body.style.opacity = "0";
    setTimeout(() => {
        document.body.style.transition = "opacity 0.5s ease-in";
        document.body.style.opacity = "1";
    }, 10);
});
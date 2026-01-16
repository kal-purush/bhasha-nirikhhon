const profileBtn = document.querySelector(".profile");
const profileToggle = document.querySelector(".profile_box");


profileBtn.addEventListener('click', () => {
    profileToggle.classList.toggle("display_profile");
})
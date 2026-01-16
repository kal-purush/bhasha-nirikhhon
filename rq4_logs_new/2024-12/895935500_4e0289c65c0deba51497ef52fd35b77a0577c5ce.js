// 관리자 로그아웃
const adminLogoutBtn = document.querySelector("#admin_logout");
adminLogoutBtn.addEventListener("click", () => {
    location.href = "/logout";
})
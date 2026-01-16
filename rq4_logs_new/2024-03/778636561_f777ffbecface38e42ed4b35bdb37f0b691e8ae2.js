//Sections elements
const header = document.querySelector(".header");
const nav = document.querySelector(".container-nav");
const homeEl = document.querySelector("#home");
const aboutEl = document.querySelector("#about");
const shopEl = document.querySelector("#shop");
//Hero section
const welcomeText = document.querySelector(".welcome-text");
//Modal mechanics elements
const crtAccBtnEl = document.querySelector(".crt-acc-btn");
const formAccEl = document.querySelector(".crt-acc-form");
const overlayEl = document.querySelector(".overlay");
//Create form elements
const signinContainer = document.querySelector(".signin-container");
const loginContainer = document.querySelector(".login-container");
const containers = [signinContainer, loginContainer];
//SIGN IN
const emailInputEl = document.querySelector(".email-input");
const usernameInputEl = document.querySelector(".username");
const usernameLabel = document.querySelector(".username-label");
const passwordInputEl = document.querySelector(".password-input");
const rePasswordInputEl = document.querySelector(".re-password-input");
const allInputEl = formAccEl.querySelectorAll("input");
const crtBtnFormEl = document.querySelector(".crt-acc-btn-form");
const toLoginBtnEl = document.querySelector(".to-login-btn");
const crtAccSucessModal = document.querySelector(".crt-acc-success");
const successContinueBtn = document.querySelector(".success-continue");
//Eye Elements
const eyeBtnEl = document.querySelectorAll(".eye-btn");
const eyeIconEl = document.querySelectorAll(".fa-solid.fa-eye-slash");
const passwordEye = document.querySelector(".eye-pw");
const rePasswordEye = document.querySelector(".eye-rpw");
//LOGIN
const emailInputLogin = document.querySelector(".email-input-login");
const passwordInputLogin = document.querySelector(".password-input-login");
const loginBtn = document.querySelector(".login-btn");
const toSigninBtnEl = document.querySelector(".to-signin-btn");
const loginPasswordEye = document.querySelector(".eye-lpw");
const checkLogin = document.querySelector(".check-login");
const checkCreateAcc = document.querySelector(".check-crt-acc");
//LOGOUT
const logoutBtn = document.querySelector(".logout-btn");
const logoutVerification = document.querySelector(".logout-verification");
const logoutCancel = document.querySelector(".logout-cancel");
const logoutVerify = document.querySelector(".logout-verify");

const toBtns = [toLoginBtnEl, toSigninBtnEl];

//Scroll to view functions
const scrollToHome = () => {
  homeEl.scrollIntoView({ behavior: "smooth" });
};
const scrollToCollection = () => {
  shopEl.scrollIntoView({ behavior: "smooth" });
};
const scrollToAbout = () => {
  aboutEl.scrollIntoView({ behavior: "smooth" });
};

//Create Account Modal

crtAccBtnEl.addEventListener("click", function () {
  formAccEl.style.display = "flex";
  overlayEl.style.display = "block";
  emailInputEl.focus();
});

overlayEl.addEventListener("click", function () {
  crtAccSucessModal.classList.add("hidden");
  checkLogin.classList.add("hidden");
  checkCreateAcc.classList.add("hidden");
  logoutVerification.classList.add("hidden");
  formAccEl.style.display = "none";
  overlayEl.style.display = "none";

  allInputEl.forEach((input) => {
    input.value = "";
  });
});

//Local storage

const accounts = JSON.parse(localStorage.getItem("accounts")) || [];

const saveAccountsToLocalStorage = () => {
  localStorage.setItem("accounts", JSON.stringify(accounts));
};

// accounts.splice(6, 1);
// saveAccountsToLocalStorage();
console.log(accounts);

//Add account function
const addAccount = (email, password, username) => {
  const acc = { email: email, password: password, username: username };
  accounts.push(acc);
  saveAccountsToLocalStorage();
  console.log(accounts);
};

//Button form
crtBtnFormEl.addEventListener("click", function (e) {
  e.preventDefault();

  const email = emailInputEl.value.trim();
  const username = usernameInputEl.value.trim();
  const password = passwordInputEl.value.trim();
  const rePassword = rePasswordInputEl.value.trim();
  //elements@elements.elements
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

  if (
    !emailRegex.test(email) ||
    password.length < 5 ||
    password !== rePassword ||
    email === "" ||
    password === ""
  ) {
    checkCreateAcc.classList.remove("hidden");
    return;
  }

  checkCreateAcc.classList.add("hidden");
  formAccEl.style.display = "none";
  crtAccSucessModal.classList.remove("hidden");
  overlayEl.style.display = "block";
  addAccount(email, password, username);

  allInputEl.forEach((input) => {
    input.value = "";
    console.log(email, password, username);
  });
});

successContinueBtn.addEventListener("click", () => {
  crtAccSucessModal.classList.add("hidden");
  overlayEl.style.display = "none";
});

window.addEventListener("keydown", function (e) {
  if (e.key == "Escape") {
    formAccEl.style.display = "none";
    overlayEl.style.display = "none";
    allInputEl.forEach((input) => {
      input.value = "";
    });
  }
});

//Eye functions
eyeBtnEl.forEach((eye) => {
  eye.addEventListener("click", function (e) {
    e.preventDefault();
  });
});

passwordEye.addEventListener("click", function (e) {
  const eyeIcon = e.target.classList.value.split(" ").includes("fa-solid");

  if (eyeIcon) {
    if (passwordInputEl.getAttribute("type") === "password")
      passwordInputEl.type = "text";
    else passwordInputEl.type = "password";
  }
});

rePasswordEye.addEventListener("click", function (e) {
  const eyeIcon = e.target.classList.value.split(" ").includes("fa-solid");

  if (eyeIcon) {
    if (rePasswordInputEl.getAttribute("type") === "password")
      rePasswordInputEl.type = "text";
    else rePasswordInputEl.type = "password";
  }
});

eyeIconEl.forEach((eye) => {
  eye.addEventListener("click", function (e) {
    this.classList.toggle("fa-eye");
    this.classList.toggle("fa-eye-slash");
  });
});

toBtns.forEach((btn) => {
  btn.addEventListener("click", function (e) {
    e.preventDefault();
    containers.forEach((container) => {
      container.classList.toggle("hidden");
    });
  });
});

loginPasswordEye.addEventListener("click", function () {
  if (passwordInputLogin.getAttribute("type") === "password")
    passwordInputLogin.type = "text";
  else passwordInputLogin.type = "password";
});

//LOG IN
let currentAcc;

const welcomeTextChange = (currAcc) => {
  welcomeText.textContent = `Hi, ${currAcc.username}! Welcome to Elavare,`;
};

loginBtn.addEventListener("click", function (e) {
  e.preventDefault();

  const email = emailInputLogin.value.trim();
  const password = passwordInputLogin.value.trim();

  currentAcc = accounts.find((acc) => acc.email == email);
  if (currentAcc && currentAcc.password == password) {
    checkLogin.classList.add("hidden");
    emailInputLogin.value = "";
    passwordInputLogin.value = "";
    formAccEl.style.display = "none";
    overlayEl.style.display = "none";
    console.log("Login Successful!");
    crtAccBtnEl.classList.add("hidden");
    logoutBtn.classList.remove("hidden");
    usernameLabel.classList.remove("hidden");
    usernameLabel.textContent = currentAcc.username;
    welcomeTextChange(currentAcc);
    console.log(currentAcc);
  } else {
    checkLogin.classList.remove("hidden");
  }
});

logoutBtn.addEventListener("click", function () {
  logoutVerification.classList.remove("hidden");
  overlayEl.style.display = "block";
  // crtAccBtnEl.classList.remove("hidden");
  // logoutBtn.classList.add("hidden");
  // usernameLabel.classList.add("hidden");
  // usernameLabel.textContent = "";
  // welcomeText.textContent = `Welcome to Elavare,`;
});

logoutVerify.addEventListener("click", () => {
  logoutVerification.classList.add("hidden");
  overlayEl.style.display = "none";
  crtAccBtnEl.classList.remove("hidden");
  logoutBtn.classList.add("hidden");
  usernameLabel.classList.add("hidden");
  usernameLabel.textContent = "";
  welcomeText.textContent = `Welcome to Elavare,`;
});

logoutCancel.addEventListener("click", () => {
  logoutVerification.classList.add("hidden");
  overlayEl.style.display = "none";
});

//Scroll nav pop-up
const navHeight = nav.getBoundingClientRect().height;

const stickyNav = (entries) => {
  const [entry] = entries;
  // console.log(entry);
  if (!entry.isIntersecting) nav.classList.add("sticky");
  else nav.classList.remove("sticky");
};

const navObserver = new IntersectionObserver(stickyNav, {
  root: null,
  rootMargin: `-${navHeight}px`,
  threshold: 0,
});
navObserver.observe(header);
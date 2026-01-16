const id = document.getElementById("username");
const loginBtn = document.getElementById("login_btn");
const loginArea = document.querySelector(".login_area");
const bgArea = document.querySelector(".bg_el_cont");

loginBtn.addEventListener("click", (e) => {
  const idValue = id.value;

  e.preventDefault();

  if (idValue.length !== 0) {
    const goLink = () => {
      window.location.href = "./todo.html";
    };
    localStorage.removeItem("idValue");
    localStorage.setItem("idValue", idValue);

    loginArea.innerHTML = `<p class="hello_text">WELCOME ${idValue}!</p>`;

    setTimeout(goLink, 1500);
  } else {
    alert("이름을 입력해주세요.");
  }
});

function backgroundChange() {
  const randomNum = Math.round(Math.random());

  if (randomNum) {
    remove;
    loginArea.classList.add("blue");
    bgArea.innerHTML = `<img class="el1" src="./assets/imgs/bl_1.png" alt="배경이미지 요소1" />
    <img class="el2" src="./assets/imgs/bl_2.png" alt="배경이미지 요소2" />
    <img class="el3" src="./assets/imgs/bl_3.png" alt="배경이미지 요소3" />`;
  } else {
    remove;
    loginArea.classList.add("purple");
    bgArea.innerHTML = `<img class="el1" src="./assets/imgs/pu_1.png" alt="배경이미지 요소1" />
    <img class="el2" src="./assets/imgs/pu_2.png" alt="배경이미지 요소2" />
    <img class="el3" src="./assets/imgs/pu_3.png" alt="배경이미지 요소3" />`;
  }

  function remove() {
    remove = loginArea.classList.remove("blue");
    loginArea.classList.remove("pulple");
  }
}

backgroundChange();
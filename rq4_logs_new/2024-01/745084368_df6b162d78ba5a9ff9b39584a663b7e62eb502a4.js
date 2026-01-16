var typeData = new Typed(".Role", {
  strings: [
    "Your Health, Your Guide: Introducing Consult Catalyst.",
    "Innovative Solution for a Healthier Tomorrow.",
    "Genuine treatments for your Health.",
    "Symptoms Unveiled: The Consult Catalyst.",
  ],
  loop: true,
  typeSpeed: 30,
  backSpeed: 40,
  backDelay: 1000,
});

const themeIcon = document.getElementById("theme-icon");
const sunIcon = document.querySelector(".sun-icon");
const moonIcon = document.querySelector(".moon-icon");
const changeTheme = document.querySelector(".change-theme");
const changeTheme2 = document.querySelector(".change-theme-two");
const sendMsg = document.querySelector("[data-sendMsg]");

themeIcon.addEventListener("click", () => {
  changeTheme.classList.toggle("fa-moon-o");
  changeTheme2.classList.toggle("bg-white");
  changeTheme2.classList.toggle("text-black");
});

function sendMessage() {
  const request = new XMLHttpRequest();
  request.open(
    "POST",
    "https://discord.com/api/webhooks/1168627256487313441/gplOfnVhj7CYQwoYoyNciKWOteSs8UWg9AqJN3zAdDLcHi4NjLF6c32YhLCW-w0taGD_"
  );

  request.setRequestHeader("Content-type", "application/json");

  const params = {
    username: (Myname = document.getElementById("name").value),
    avatar_url: "",
    content: (message = document.getElementById("message").value),
  };

  request.send(JSON.stringify(params));

  try {
    sendMsg.innerText = "Message Sent!";
  } catch (e) {
    sendMsg.innerText = "Failed";
  }
  //to make copy wala span visible
  sendMsg.classList.add("active");

  setTimeout(() => {
    sendMsg.classList.remove("active");
  }, 2000);
}

document.addEventListener("DOMContentLoaded", function () {
  var arrow = document.getElementById("arrow");

  window.addEventListener("scroll", function () {
    if (window.scrollY > 300) {
      arrow.style.display = "block";
    } else {
      arrow.style.display = "none";
    }
  });

  arrow.addEventListener("click", function () {
    scrollToTop();
  });

  function scrollToTop() {
    window.scrollTo({
      top: 0,
      behavior: "smooth",
    });
  }
});

async function checkSymptoms() {
  const url =
    "https://symptom-checker4.p.rapidapi.com/analyze?symptoms=%3CREQUIRED%3E";
  const options = {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "X-RapidAPI-Key": "ed051a50a4msh811a1a6221c6aa2p1f7230jsnbc42b9ee58cb",
      "X-RapidAPI-Host": "symptom-checker4.p.rapidapi.com",
    },
    body: {
      symptoms:
        "I have a red rash on my forearm that appeared suddenly last night. It does not itch or hurt.",
    },
  };

  try {
    const response = await fetch(url, options);
    const result = await response.text();
    console.log(result);
  } catch (error) {
    console.error(error);
  }
}

function displayResults(response) {
  const resultDiv = document.getElementById("result");

  // Clear previous results
  resultDiv.innerHTML = "";

  if (data.length === 0) {
    resultDiv.textContent =
      "No matching diseases found for the given symptoms.";
  } else {
    const resultList = document.createElement("ul");

    data.forEach((disease) => {
      const listItem = document.createElement("li");
      listItem.textContent = disease;
      resultList.appendChild(listItem);
    });

    resultDiv.appendChild(resultList);
  }
}
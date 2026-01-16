const CHANNEL_ID = "2954780";
const API_KEY = "SF3SUTTKUIZ59XZ4";

const THRESHOLDS = {
  temperature: 30,
  humidity: 10,
  co: 10,
  smoke: 1
};

const beepSound = new Audio("sounds/beep.mp3");

const elements = {
  temp: document.getElementById("temp"),
  humidity: document.getElementById("humidity"),
  co: document.getElementById("co"),
  smoke: document.getElementById("smoke"),
  alerts: {
    temp: document.getElementById("temp-alert"),
    humidity: document.getElementById("humidity-alert"),
    co: document.getElementById("co-alert"),
    smoke: document.getElementById("smoke-alert")
  },
  tableBody: document.getElementById("data-body")
};

let activeAlerts = new Set();

// Beep every 2 seconds if any alert is active
setInterval(() => {
  if (activeAlerts.size > 0) {
    beepSound.play();
  }
}, 2000);

function showNotification(message) {
  if (Notification.permission === "granted") {
    new Notification("Environment Alert 🚨", {
      body: message,
      icon: "images/alert-icon.png"
    });
  }
}

function showAlert(sensor, message) {
  const alertBox = elements.alerts[sensor];
  alertBox.textContent = `⚠️ ${message}`;
  alertBox.style.display = "flex";
  activeAlerts.add(sensor);
  showNotification(message);
}

function hideAlerts() {
  for (let key in elements.alerts) {
    elements.alerts[key].style.display = "none";
  }
  activeAlerts.clear();
}

function updateValues(latest) {
  const temperature = parseFloat(latest.field1);
  const humidity = parseFloat(latest.field2);
  const co = parseFloat(latest.field3);
  const smoke = parseFloat(latest.field4);

  elements.temp.textContent = `${temperature.toFixed(1)} °C`;
  elements.humidity.textContent = `${humidity.toFixed(1)} %`;
  elements.co.textContent = `${co.toFixed(1)} ppm`;
  elements.smoke.textContent = `${smoke.toFixed(1)} ppm`;

  hideAlerts();

  if (temperature > THRESHOLDS.temperature) showAlert("temp", "High Temperature Detected");
  if (humidity > THRESHOLDS.humidity) showAlert("humidity", "High Humidity Level");
  if (co > THRESHOLDS.co) showAlert("co", "Dangerous Carbon Monoxide Level");
  if (smoke > THRESHOLDS.smoke) showAlert("smoke", "Smoke Level Elevated");
}

function updateTable(feeds) {
  elements.tableBody.innerHTML = "";
  feeds.reverse().forEach((entry, index) => {
    const row = document.createElement("tr");
    if (index === 0) row.classList.add("latest");
    row.innerHTML = `
      <td>${new Date(entry.created_at).toLocaleString()}</td>
      <td>${parseFloat(entry.field1).toFixed(1)}</td>
      <td>${parseFloat(entry.field2).toFixed(1)}</td>
      <td>${parseFloat(entry.field3).toFixed(1)}</td>
      <td>${parseFloat(entry.field4).toFixed(1)}</td>
    `;
    elements.tableBody.appendChild(row);
  });
}

function fetchData() {
  const url = `https://api.thingspeak.com/channels/${CHANNEL_ID}/feeds.json?api_key=${API_KEY}&results=4&_=${new Date().getTime()}`;

  fetch(url)
    .then(res => res.json())
    .then(data => {
      if (!data.feeds || data.feeds.length === 0) throw new Error("No data found");
      const feeds = data.feeds;
      const latest = feeds[feeds.length - 1];
      updateValues(latest);
      updateTable(feeds);
    })
    .catch(err => {
      console.error("Fetch error:", err);
      hideAlerts();
    });
}

// DateTime Display
function updateDateTime() {
  const now = new Date();
  const formatted = now.toLocaleString();
  document.getElementById("datetime").textContent = formatted;
}

setInterval(updateDateTime, 1000);
updateDateTime();

// Request Notification Permission on Load
if (Notification.permission !== "granted") {
  Notification.requestPermission();
}

// Fetch data every 15 seconds
fetchData();
setInterval(fetchData, 15000);


document.getElementById("sendEmailBtn").addEventListener("click", () => {
  const recipientEmail = document.getElementById("recipientEmail").value;
  const statusDiv = document.getElementById("emailStatus");

  if (!recipientEmail || !recipientEmail.includes("@")) {
    statusDiv.textContent = "❌ Please enter a valid email address.";
    return;
  }

  const currentData = {
    temperature: elements.temp.textContent,
    humidity: elements.humidity.textContent,
    co: elements.co.textContent,
    smoke: elements.smoke.textContent,
    alerts: Array.from(activeAlerts).map(sensor => elements.alerts[sensor].textContent)
  };

  const formData = new FormData();
  formData.append("to_email", recipientEmail);
  formData.append("message", `
    Temperature: ${currentData.temperature}
    Humidity: ${currentData.humidity}
    CO: ${currentData.co}
    Smoke: ${currentData.smoke}
    Alerts: ${currentData.alerts.length > 0 ? currentData.alerts.join(", ") : "No alerts"}
    Time: ${new Date().toLocaleString()}
    System: EcoAir_Sense
  `);

  statusDiv.textContent = "📤 Sending email...";

  fetch("https://formspree.io/f/mrbqwywz", {
    method: "POST",
    body: formData,
    headers: {
      Accept: "application/json"
    }
  })
    .then(res => res.json())
    .then(data => {
      if (data.ok) {
        statusDiv.textContent = "✅ Email sent successfully!";
      } else {
        throw new Error(data.error || "Unknown error");
      }
    })
    .catch(err => {
      console.error("Email error:", err);
      statusDiv.textContent = "❌ Failed to send email. Try again.";
    });
});
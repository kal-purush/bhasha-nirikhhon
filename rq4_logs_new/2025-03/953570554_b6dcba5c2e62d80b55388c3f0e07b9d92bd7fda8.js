const socket = new WebSocket("ws://192.168.1.107/ws");  // Replace with ESP8266 IP

socket.addEventListener("open", () => {
    console.log("✅ Connected to WebSocket Server");
});

socket.addEventListener("message", (event) => {
    try {
        const data = JSON.parse(event.data);
        document.getElementById("room_temp").innerText = `${data.room_temp}°C`;
        document.getElementById("humidity").innerText = `${data.humidity}%`;
        document.getElementById("heart_rate").innerText = `${data.heart_rate} BPM`;
        document.getElementById("blood_oxygen").innerText = `${data.blood_oxygen}%`;
        document.getElementById("body_temp").innerText = `${data.body_temp}°C`;
        console.log("📥 Received Data:", data);
    } catch (error) {
        console.error("❌ Error parsing JSON:", error);
    }
});

socket.addEventListener("error", (error) => {
    console.error("❌ WebSocket Error:", error);
});

socket.addEventListener("close", () => {
    console.log("🔌 WebSocket Disconnected. Reconnecting...");
    setTimeout(() => location.reload(), 3000);  // Auto-reconnect after 3 sec
});
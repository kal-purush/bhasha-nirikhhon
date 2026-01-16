function updateClock() {
    // Get the current time and date
    const now = new Date();
    const hours = String(now.getHours()).padStart(2, "0");
    const minutes = String(now.getMinutes()).padStart(2, "0");
    const seconds = String(now.getSeconds()).padStart(2, "0");
    // Format the time and date
    const time = `${hours}:${minutes}:${seconds}`;
    const date = now.toLocaleDateString("en-US", {
      year: "numeric",
      month: "short",
      day: "numeric",
    });
    // Update the HTML elements
    document.getElementById("time").textContent = time;
    document.getElementById("date").textContent = date;
  }
  // Update the clock every second
  setInterval(updateClock, 1000);
  // Initial call to display the clock immediately
  updateClock();
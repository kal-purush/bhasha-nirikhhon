 // Set the date for the countdown (for example, New Year's Eve)
const countdownDate = new Date("2025-01-01T00:00:00").getTime();

// Update the countdown every second
const interval = setInterval(function() {
    const now = new Date().getTime();
    const distance = countdownDate - now;

    // Calculate days, hours, minutes, and seconds
    const days = Math.floor(distance / (1000 * 60 * 60 * 24));
    const hours = Math.floor((distance % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60));
    const minutes = Math.floor((distance % (1000 * 60 * 60)) / (1000 * 60));
    const seconds = Math.floor((distance % (1000 * 60)) / 1000);

    // Display the result
    document.getElementById("days").textContent = days;
    document.getElementById("hours").textContent = hours;
    document.getElementById("minutes").textContent = minutes;
    document.getElementById("seconds").textContent = seconds;

    // If the countdown is finished, display a message and perform a task
    if (distance < 0) {
        clearInterval(interval);
        document.getElementById("countdown").innerHTML = "Countdown Ended!";
        
        // Call the custom function when countdown ends
        performTask();
    }
}, 1000);

// Define the function to be called when the countdown ends
function performTask() {
    console.log("The countdown is complete. Performing task...");
    
    // Example task: You can replace this with any task you want to perform
     
    window.location.href = "index.html";
    
    // Additional tasks can be added here
}
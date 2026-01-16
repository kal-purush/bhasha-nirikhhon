
        document.addEventListener("DOMContentLoaded", function () {
            const currentTimeElement = document.getElementById("currentTime");
            
            function displayCurrentTimeInMilliseconds() {
                const currentTimeInMilliseconds = new Date().getTime();
                currentTimeElement.textContent = currentTimeInMilliseconds.toString();
            }

            // Display the current time initially
            displayCurrentTimeInMilliseconds();

            // Update the time every second (1000 milliseconds)
            setInterval(displayCurrentTimeInMilliseconds, 1000);
            
        });

        document.addEventListener("DOMContentLoaded", function () {
            const currentDayElement = document.getElementById("currentDay");
            
            // Array of days of the week
            const daysOfWeek = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];

            // Get the current day of the week (0 = Sunday, 1 = Monday, ..., 6 = Saturday)
            const currentDayIndex = new Date().getDay();

            // Get the name of the current day
            const currentDayName = daysOfWeek[currentDayIndex];

            // Set the current day as an attribute value
            currentDayElement.setAttribute("data-testid", currentDayName);
            currentDayElement.textContent = currentDayName;
        });
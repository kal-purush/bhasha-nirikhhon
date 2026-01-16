const login = document.getElementById("login");
const signup = document.getElementById("signup");
const contact = document.getElementById("contact");
const description = document.getElementById("description");
const myData = document.getElementById("data");
const sidebar = document.querySelector(".sidebar");
const reset = document.getElementById("reset");

// Function to animate the sidebar to a button
function animateSidebarToButton(button, userDescription) {
    const buttonRect = button.getBoundingClientRect();
    
    myData.innerText = userDescription;
    // Set the sidebar's position to absolute
    sidebar.style.position = "absolute";

    // Animate the transition
    sidebar.style.transition = "top 0.5s, right 0.5s, left 0.5s";

    // Set the sidebar's position next to the clicked button
    sidebar.style.top = `${buttonRect.top + 80}px`;
    sidebar.style.right = `${window.innerWidth - buttonRect.right}px`;
    sidebar.style.left = `${buttonRect.left}px`;

    // Trigger a layout change to apply the animation
    void sidebar.offsetWidth;
    
    // After the animation, you can update the content or perform other actions
    setTimeout(() => {
        // Your code here
    }, 500); // Adjust the time as needed
}
function resetSidebar() {
    sidebar.style.position = "fixed";
    sidebar.style.bottom = "20px";
    sidebar.style.right = "20px";
    sidebar.style.left = "";
    sidebar.style.top = ""
    myData.innerText = "I Am Your Assistance";
}

login.addEventListener("click", () => {
    animateSidebarToButton(login,"This is the button for Login");
});

signup.addEventListener("click", () => {
    animateSidebarToButton(signup,"This is the button for Signup");
});

contact.addEventListener("click", () => {
    animateSidebarToButton(contact,"This is the button for Contact");
});

description.addEventListener("click", () => {
    animateSidebarToButton(description,"This is the button for Description");
});
reset.addEventListener("click", () => {
    resetSidebar();
});
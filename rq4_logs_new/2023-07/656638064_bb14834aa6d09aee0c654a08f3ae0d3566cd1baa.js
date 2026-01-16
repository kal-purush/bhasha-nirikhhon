// JavaScript for filter functionality
const filterInput = document.getElementById("filter");
const skillsList = document.querySelector(".skills-list");

filterInput.addEventListener("input", function () {
  const searchTerm = filterInput.value.toLowerCase();
  const skills = Array.from(skillsList.children);

  skills.forEach((skill) => {
    const skillName = skill.textContent.toLowerCase();
    if (skillName.includes(searchTerm)) {
      skill.style.display = "inline-block";
    } else {
      skill.style.display = "none";
    }
  });
});

// // To check 
// // JavaScript for smooth scrolling when clicking on navigation links
// $(document).ready(function () {
//   $("a").on("click", function (event) {
//       if (this.hash !== "") {
//           event.preventDefault();
//           var hash = this.hash;
//           $("html, body").animate(
//               {
//                   scrollTop: $(hash).offset().top,
//               },
//               800,
//               function () {
//                   window.location.hash = hash;
//               }
//           );
//       }
//   });
// });































// ... Existing JavaScript code ...

// Function to display the contact popup
function openContactPopup() {
  var popup = document.getElementById("contact-popup");
  popup.style.display = "block";
}

// Function to close the contact popup
function closeContactPopup() {
  var popup = document.getElementById("contact-popup");
  popup.style.display = "none";
}

// Function to toggle visibility of the education and skills sections
function toggleSectionVisibility(sectionId) {
  var section = document.getElementById(sectionId);
  if (section.style.display === "none") {
      section.style.display = "block";
  } else {
      section.style.display = "none";
  }
}

// ... Remaining JavaScript code ...
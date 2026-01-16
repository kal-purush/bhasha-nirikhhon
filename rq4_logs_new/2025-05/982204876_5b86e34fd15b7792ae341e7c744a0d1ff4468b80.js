let artsDetails = [
  { image: "", courseName: "Class 11th", currentPrice: "20000", realPrice: "24000" },
  { image: "", courseName: "Class 12th", currentPrice: "25000", realPrice: "32500" }
];

let arts = document.querySelector(".courses-container-list");
arts.innerHTML = "";

artsDetails.forEach(function (course) {
  let discountPercentage = ((course.realPrice - course.currentPrice) / course.realPrice) * 100;

  arts.innerHTML += `
  <div class="course-element">
    <img src="https://images.unsplash.com/photo-1509062522246-3755977927d7?w=500&auto=format&fit=crop&q=60&ixlib=rb-4.0.3&ixid=M3wxMjA3fDB8MHxzZWFyY2h8Mnx8Y2xhc3Nyb29tfGVufDB8fDB8fHww" alt="">
    <div class="course-details">
      <span class="name">${course.courseName}</span>
      <div class="price-container">
        <span class="special">Special Discounted Price</span>
        <div class="price">
          <span class="current-price">₹ ${course.currentPrice}/year</span>
          <span class="real-price">₹ ${course.realPrice}/year</span>
          <span class="discount">${Math.floor(discountPercentage)}% OFF</span>
        </div>
      </div>
      <a href="">Get started</a>
    </div>
  </div>
  `;
});
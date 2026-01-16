// Mock menu items with real images and customization
const menuItems = [
  { name: "Margherita Pizza", price: 8, img: "https://example.com/images/margherita.jpg", customization: ['Extra Cheese', 'Gluten-Free'] },
  { name: "Pepperoni Pizza", price: 9, img: "https://example.com/images/pepperoni.jpg", customization: ['Spicy', 'Extra Pepperoni'] },
  { name: "Pasta Carbonara", price: 12, img: "https://example.com/images/carbonara.jpg", customization: ['Extra Sauce', 'No Bacon'] },
  { name: "Caesar Salad", price: 7, img: "https://example.com/images/caesar-salad.jpg", customization: ['Add Chicken', 'Extra Dressing'] },
  { name: "Veggie Burger", price: 10, img: "https://example.com/images/veggie-burger.jpg", customization: ['Vegan Cheese', 'No Mayo'] },
  { name: "Grilled Chicken", price: 15, img: "https://example.com/images/grilled-chicken.jpg", customization: ['Extra Spicy', 'Gluten-Free'] },
  { name: "Fish Tacos", price: 13, img: "https://example.com/images/fish-tacos.jpg", customization: ['Extra Lime', 'No Sauce'] },
  { name: "Chocolate Lava Cake", price: 5, img: "https://example.com/images/lava-cake.jpg", customization: [] },
  { name: "Ice Cream Sundae", price: 6, img: "https://example.com/images/ice-cream-sundae.jpg", customization: ['Extra Sprinkles', 'No Nuts'] },
  { name: "Tiramisu", price: 7, img: "https://example.com/images/tiramisu.jpg", customization: [] }
];
// Populate the digital menu with customization options and pictures
function populateDigitalMenu() {
  const menuContainer = document.getElementById('menuItems');
  menuContainer.innerHTML = ''; // Clear existing items

  menuItems.forEach(item => {
    const menuItemDiv = document.createElement('div');
    menuItemDiv.classList.add('menu-item');

    const img = document.createElement('img');
    img.src = item.img; // Add image from the menu item
    img.alt = item.name;

    const label = document.createElement('label');
    label.innerHTML = `<input type="checkbox" value="${item.name},${item.price}"> ${item.name} - $${item.price}`;

    const customization = document.createElement('select');
    customization.innerHTML = `<option value="">No customization</option>`;
    item.customization.forEach(option => {
      customization.innerHTML += `<option value="${option}">${option}</option>`;
    });

    menuItemDiv.appendChild(img);
    menuItemDiv.appendChild(label);
    menuItemDiv.appendChild(customization);
    menuContainer.appendChild(menuItemDiv);
  });
}
document.addEventListener("DOMContentLoaded", () => {
  let queuePosition = 0;
  let totalQueue = 5; // Assume 5 people already in the queue
  let isTableAvailable = true; // Mock table availability
  let notificationMessage = '';
  let selectedItems = [];

  // Mock menu items with images and customization
  const menuItems = [
    { name: "Margherita Pizza", price: 8, img: "https://via.placeholder.com/100", customization: ['Extra Cheese', 'Gluten-Free'] },
    { name: "Pepperoni Pizza", price: 9, img: "https://via.placeholder.com/100", customization: ['Spicy', 'Extra Pepperoni'] },
    { name: "Pasta Carbonara", price: 12, img: "https://via.placeholder.com/100", customization: ['Extra Sauce', 'No Bacon'] },
    { name: "Caesar Salad", price: 7, img: "https://via.placeholder.com/100", customization: ['Add Chicken', 'Extra Dressing'] },
    { name: "Veggie Burger", price: 10, img: "https://via.placeholder.com/100", customization: ['Vegan Cheese', 'No Mayo'] },
    { name: "Grilled Chicken", price: 15, img: "https://via.placeholder.com/100", customization: ['Extra Spicy', 'Gluten-Free'] },
    { name: "Fish Tacos", price: 13, img: "https://via.placeholder.com/100", customization: ['Extra Lime', 'No Sauce'] },
    { name: "Chocolate Lava Cake", price: 5, img: "https://via.placeholder.com/100", customization: [] },
    { name: "Ice Cream Sundae", price: 6, img: "https://via.placeholder.com/100", customization: ['Extra Sprinkles', 'No Nuts'] },
    { name: "Tiramisu", price: 7, img: "https://via.placeholder.com/100", customization: [] }
  ];

  // Populate the digital menu with customization options
  function populateDigitalMenu() {
    const menuContainer = document.getElementById('menuItems');
    menuContainer.innerHTML = ''; // Clear existing items

    menuItems.forEach(item => {
      const menuItemDiv = document.createElement('div');
      menuItemDiv.classList.add('menu-item');

      const img = document.createElement('img');
      img.src = item.img;

      const label = document.createElement('label');
      label.innerHTML = `<input type="checkbox" value="${item.name},${item.price}"> ${item.name} - $${item.price}`;

      const customization = document.createElement('select');
      customization.innerHTML = `<option value="">No customization</option>`;
      item.customization.forEach(option => {
        customization.innerHTML += `<option value="${option}">${option}</option>`;
      });

      menuItemDiv.appendChild(img);
      menuItemDiv.appendChild(label);
      menuItemDiv.appendChild(customization);
      menuContainer.appendChild(menuItemDiv);
    });
  }

  populateDigitalMenu();

  // Personalized Table Booking
  document.getElementById('bookTableBtn').addEventListener('click', () => {
    const tablePreference = document.getElementById('tablePreference').value;
    if (isTableAvailable) {
      document.getElementById('bookingStatus').innerText = `Table booked! Preference: ${tablePreference}`;
      sendNotification(`Your table with ${tablePreference} preference is ready!`);
    } else {
      document.getElementById('bookingStatus').innerText = 'No tables available, joining virtual queue...';
      joinQueue();
    }
  });

  // Virtual Queue Logic
  function joinQueue() {
    queuePosition = totalQueue + 1;
    totalQueue++;
    updateQueueStatus();
  }

  function updateQueueStatus() {
    document.getElementById('queuePosition').innerText = `Your position in the queue: ${queuePosition}`;
    document.getElementById('estimatedWait').innerText = `Estimated wait time: ${queuePosition * 10} minutes`;

    if (queuePosition === 1) {
      isTableAvailable = true;
      sendNotification('Your table is ready!');
      document.getElementById('queueStatus').innerText = 'You are next!';
    } else {
      isTableAvailable = false;
      document.getElementById('queueStatus').innerText = 'Waiting for a table...';
    }
  }

  // Pre-Order
  document.getElementById('preOrderBtn').addEventListener('click', () => {
    selectedItems = [];
    const checkboxes = document.querySelectorAll('#menuItems input[type="checkbox"]:checked');
    checkboxes.forEach(checkbox => {
      selectedItems.push(checkbox.value.split(',')[0]);
    });

    if (selectedItems.length > 0) {
      document.getElementById('preOrderStatus').innerText = `Pre-ordered: ${selectedItems.join(', ')}`;
      sendNotification('Your order has been placed.');
    } else {
      document.getElementById('preOrderStatus').innerText = 'No items selected for pre-order.';
    }
  });

  // Notification System
  function sendNotification(message) {
    notificationMessage = message;
    document.getElementById('notificationMessage').innerText = notificationMessage;
  }

  // UPI Payment
  document.getElementById('upiPayBtn').addEventListener('click', () => {
    const upiId = document.getElementById('upiId').value;
    if (upiId) {
      document.getElementById('upiPaymentStatus').innerText = `Payment successful via UPI: ${upiId}`;
    } else {
      document.getElementById('upiPaymentStatus').innerText = 'Please enter a valid UPI ID.';
    }
  });

  // Populate Popular Menu
  function PopularMenu() {
    const popularMenu = document.getElementById('popularMenu');
    popularMenu.innerHTML = '<li>Pizza</li><li>Burger</li><li>Caesar Salad</li><li>Pasta Carbonara</li>';
  }
  PopularMenu();

  // Initialize Crowd Analytics Chart
  const ctx = document.getElementById('crowdChart').getContext('2d');
  const crowdChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: ['12 PM', '1 PM', '2 PM', '3 PM', '4 PM'],
      datasets: [{
        label: 'Number of Customers',
        data: [5, 15, 30, 20, 10],
        backgroundColor: 'rgba(75, 192, 192, 0.6)',
      }]
    },
    options: {
      scales: {
        y: {
          beginAtZero: true
        }
      }
    }
  });

  // Update Analytics Insights
  document.getElementById('peakTime').innerText = 'Peak Time: 1:00 PM - 2:00 PM';
  document.getElementById('popularItem').innerText = 'Most Popular Menu Item: Pizza';
  document.getElementById('tableUtilization').innerText = 'Table Utilization: 85%';
});


console.log("Slideshow script loaded");

document.addEventListener("DOMContentLoaded", () => {
  const images = document.querySelectorAll('#slideshow img');
  let currentIndex = 0;

  console.log(`Found ${images.length} images in slideshow`);

  // Set the first image as active
  if (images.length > 0) {
    images[currentIndex].classList.add('active');
  }

  function showNextImage() {
    // Hide current image
    images[currentIndex].classList.remove('active');

    // Update the index
    currentIndex = (currentIndex + 1) % images.length; // Loop back to the first image

    // Show the next image
    images[currentIndex].classList.add('active');
  }

  // Change the image every 5 seconds
  setInterval(showNextImage, 5000);
});

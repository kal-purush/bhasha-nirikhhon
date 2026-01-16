  // Firebase configuration and initialization
  
  import { initializeApp } from "https://www.gstatic.com/firebasejs/11.6.0/firebase-app.js";
  import { getDatabase, ref, push, set } from "https://www.gstatic.com/firebasejs/11.6.0/firebase-database.js";
  const firebaseConfig = {
    apiKey: "AIzaSyBRfCJKzFUsYEbP3SSEkt_x5joC7sbdngE",
    authDomain: "ktl-login-form-3ab49.firebaseapp.com",
    databaseURL: "https://ktl-login-form-3ab49-default-rtdb.firebaseio.com",
    projectId: "ktl-login-form-3ab49",
    storageBucket: "ktl-login-form-3ab49.appspot.com",
    messagingSenderId: "649057866695",
    appId: "1:649057866695:web:8682ca481d35ffd3df3fe2"
  };
  const app = initializeApp(firebaseConfig);
  const database = getDatabase(app);
  const navItems = document.querySelectorAll('.nav-item[data-target]');
  const sections = document.querySelectorAll('.section');
  navItems.forEach(item => {
    item.addEventListener('click', () => {
      const target = item.getAttribute('data-target');
      navItems.forEach(i => i.classList.remove('active'));
      sections.forEach(s => s.classList.remove('active'));
      item.classList.add('active');
      document.getElementById(target).classList.add('active');
    });
  });
  const form = document.getElementById('profileForm');
  let currentProfileKey = null;
  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    const firstName = document.getElementById('firstName').value.trim();
    const lastName = document.getElementById('lastName').value.trim();
    const phone = document.getElementById('phone').value.trim();
    const email = document.getElementById('email').value.trim();
    const address1 = document.getElementById('address1').value.trim();
    const address2 = document.getElementById('address2').value.trim();
    const city = document.getElementById('city').value.trim();
    const pincode = document.getElementById('pincode').value.trim();
    const state = document.getElementById('state').value.trim();
    if (!firstName || !lastName || !phone || !email || !address1 || !address2 || !city || !pincode || !state) {
      alert("❌ Please fill all the fields before submitting.");
      return;
    }
    try {
      const data = {
        firstName,
        lastName,
        phone,
        email,
        address1,
        address2,
        city,
        pincode,
        state,
      };
      if (currentProfileKey) {
        await set(ref(database, 'profiles/' + currentProfileKey), data);
        alert('✅ Profile updated successfully!');
      } else {
        const randomCode = Math.floor(1000000000 + Math.random() * 9000000000);
        const newRef = push(ref(database, 'profiles'));
        await set(newRef, {
          ...data,
          code: randomCode,
          createdAt: new Date().toISOString()
        });
        currentProfileKey = newRef.key;
        alert(`✅ Profile saved successfully with Code: ${randomCode}`);
        form.querySelector('button').textContent = 'Update';
      }
    } catch (error) {
      console.error("Error:", error);
      alert("❌ " + error.message);
    }
  });
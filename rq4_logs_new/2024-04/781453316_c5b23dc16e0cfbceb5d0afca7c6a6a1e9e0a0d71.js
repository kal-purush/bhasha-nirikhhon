function WhatsApp() {
    var name = document.getElementById("name").value;
    var phone = document.getElementById("phone").value;
    var streetAddress = document.getElementById("streetAddress").value;
    var city = document.getElementById("city").value;
    var postalCode = document.getElementById("postalCode").value;
    var message = document.getElementById("message").value;
    
    // Get selected food items and quantities
    var selectedItems = [];
    var items = document.querySelectorAll('input[type="checkbox"]:checked');
    items.forEach(function(item) {
      var itemName = item.getAttribute('name');
      var itemQuantity = document.getElementById(itemName + 'Quantity').value;
      selectedItems.push({ name: itemName, quantity: itemQuantity });
    });

    var WhatsAppurl = "https://wa.me/+918959065020?text="  +
      "Name Of Customer: " + name + "%0a" +
      "Mobile No: " + phone + "%0a" +
      "Address: " + streetAddress + "%0a" +
      "City: " + city + "%0a" +
      "Postal Code: " + postalCode + "%0a";

    // Add selected food items and quantities to the WhatsApp message
    selectedItems.forEach(function(item) {
      WhatsAppurl += "Food: " + item.name + " - Quantity: " + item.quantity + "%0a";
    });

    // Add additional message if provided
    WhatsAppurl += "Message: " + message;
    // Open WhatsApp link in a new tab
    var whatsappWindow = window.open(WhatsAppurl, "_blank");
}
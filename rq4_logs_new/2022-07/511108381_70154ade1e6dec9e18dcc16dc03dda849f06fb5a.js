function replacePhone() {
	var selectContactPhone = document.getElementsByName("ft-header-dropdown ft-header-contact-phones").textContent;
		var active_Elements = document.activeElement;
		var current_selection = active_Elements.value.substring(active_Elements.selectionStart, active_Elements.selectionEnd);
		var currentCountry = document.getElementsByName("country").textContent;
		var currentPhone = document.getElementsByName("phone").textContent;
		var fieldPhone = document.getElementsByName("jquery-once-4-processed").textContent;
					
		selectContactPhone.addEventListener("click", function() {
			if (currentCountry == "Australia" && currentPhone == "+61 2 6188 8118"){
				document.getElementsByName("jquery-once-4-processed").textContent = '+06-1111-1111';
				
			}
		});
			}
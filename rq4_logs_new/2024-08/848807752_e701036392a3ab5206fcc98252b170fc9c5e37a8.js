"use strict"

let currentPage = 1;
        const totalPages = 5; 

        function loadPage(page) {
            
            document.getElementById("content").innerText = `Page ${page}`;
        }

        function updateButtons() {
            const prevButton = document.getElementById("prevButton");
            const nextButton = document.getElementById("nextButton");

            if (currentPage === 1) {
                prevButton.style.display = "none"; // თუ პირველ გვერდზე ვართ, "Previous" ღილაკი არ ჩანს
            } else {
                prevButton.style.display = "inline"; // სხვა შემთხვევაში ღილაკი ჩნდება
            }

            if (currentPage === totalPages) {
                nextButton.style.display = "none"; // თუ ბოლო გვერდზე ვართ Next ღილაკი არ ჩანს
            } else {
                nextButton.style.display = "inline"; // სხვა შემთხვევაში ჩნდება
            }
        }

        function prevPage() {
            if (currentPage > 1) {
                currentPage--;
                loadPage(currentPage);
                updateButtons();
            }
        }

        function nextPage() {
            if (currentPage < totalPages) {
                currentPage++;
                loadPage(currentPage);
                updateButtons();
            }
        }

        
        loadPage(currentPage);
        updateButtons();
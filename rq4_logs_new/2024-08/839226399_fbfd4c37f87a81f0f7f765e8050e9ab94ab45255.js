document.addEventListener('DOMContentLoaded', function () {
    const birthdayForm = document.getElementById('birthdayForm');
    const wishForm = document.getElementById('wishForm');
    const messageDiv = document.getElementById('message');
    const videoContainer = document.getElementById('videoContainer');
    const submitWishButton = document.getElementById('submitWish');
    const birthdayImage = document.getElementById('birthdayImage'); 

    // Xử lý sự kiện khi gửi mã sinh nhật
    birthdayForm.addEventListener('submit', function(event) {
        event.preventDefault();
        
        const code = document.getElementById('code').value;
        if (code === "1008") {
            birthdayForm.classList.add('hidden');
            wishForm.classList.remove('hidden');
        } else {
            alert("Sai code òi! Code đã được gửi cho người sinh nhật rồi đấy.");
        }
    });

    // Xử lý sự kiện khi gửi điều ước
    submitWishButton.addEventListener('click', function(event) {
        event.preventDefault();
        
        // Thay đổi hình ảnh
        birthdayImage.classList.add('image-hidden');
        setTimeout(() => {
            birthdayImage.src = './Halade-2.png';
            birthdayImage.classList.remove('image-hidden');
            birthdayImage.classList.add('image-visible');
        }, 500);

        const wish = document.getElementById('wish').value;
        if (wish.trim() !== "") {
            wishForm.classList.add('hidden');
            messageDiv.classList.remove('hidden');
            videoContainer.classList.remove('hidden'); // Hiển thị video
            
            // Play the YouTube video
            const iframe = document.getElementById('birthdaySong');
            iframe.src += "&autoplay=1"; // Ensure autoplay is enabled
        } else {
            alert("Thôi ước đi, cả năm có mỗi dịp này được ước thôi.");
        }
    });
});
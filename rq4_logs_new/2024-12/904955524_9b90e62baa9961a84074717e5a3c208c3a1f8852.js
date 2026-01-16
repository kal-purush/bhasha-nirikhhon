document.getElementById('recommendation-form').addEventListener('submit', function (event) {
    event.preventDefault();

    // Mengambil nilai input
    const skinColor = document.getElementById('skin_color').value.trim();
    const bodyShape = document.getElementById('body_shape').value.trim();
    const height = document.getElementById('height').value.trim();
    const weight = document.getElementById('weight').value.trim();

    // Validasi input
    if (!skinColor || !bodyShape || !height || !weight) {
        alert('Semua bidang wajib diisi.');
        return;
    }

    // Format body untuk dikirim via POST
    const requestBody = new URLSearchParams({
        skin_color: skinColor,
        body_shape: bodyShape,
        height: height,
        weight: weight,
    });

    // Mengirimkan data menggunakan fetch
    fetch('http://localhost/wedding/backend/recommendation.php', {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: requestBody.toString(),
    })
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            return response.json();
        })
        .then(data => {
            const recommendationText = document.getElementById('recommendation-text');
            const recommendationImage = document.getElementById('recommendation-image');

            if (data && data.recommendation && data.image_url) {
                recommendationText.textContent = data.recommendation;
                recommendationImage.src = data.image_url;
                recommendationImage.style.display = 'block';
            } else {
                recommendationText.textContent = 'Tidak ada rekomendasi yang ditemukan.';
                recommendationImage.style.display = 'none';
            }
        })
        .catch(error => {
            console.error('Error:', error);
            alert('Terjadi kesalahan saat mengambil rekomendasi. Silakan coba lagi.');
        });
});
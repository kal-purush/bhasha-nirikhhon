document.addEventListener("DOMContentLoaded", function () {
    // Highlight the active navigation link
    const currentPage = window.location.pathname.split("/").pop(); // Get current filename
    const navLinks = document.querySelectorAll(".nav-links a");

    navLinks.forEach(link => {
        if (link.getAttribute("href") === currentPage) {
            link.classList.add("active"); // Apply active class to the current page's nav link
        }
    });

    // YouTube Thumbnail Extractor Logic
    function extractVideoId(url) {
        const patterns = [
            /(?:v=|\/)([0-9A-Za-z_-]{11}).*/, // Regular youtube.com URL
            /(?:embed\/)([0-9A-Za-z_-]{11})/, // Embed URL
            /(?:youtu\.be\/)([0-9A-Za-z_-]{11})/ // Shortened youtu.be URL
        ];

        for (const pattern of patterns) {
            const match = url.match(pattern);
            if (match) return match[1];
        }

        return null;
    }

    function getThumbnailUrls(videoId) {
        return {
            max: `https://img.youtube.com/vi/${videoId}/maxresdefault.jpg`,
            hq: `https://img.youtube.com/vi/${videoId}/hqdefault.jpg`,
            mq: `https://img.youtube.com/vi/${videoId}/mqdefault.jpg`,
            sd: `https://img.youtube.com/vi/${videoId}/sddefault.jpg`,
            default: `https://img.youtube.com/vi/${videoId}/default.jpg`
        };
    }

    async function checkImageExists(url) {
        try {
            const response = await fetch(url, { method: 'HEAD' });
            return response.ok;
        } catch {
            return false;
        }
    }

    async function downloadImage(imageUrl, quality) {
        try {
            const response = await fetch(imageUrl);
            const blob = await response.blob();

            const a = document.createElement("a");
            a.href = URL.createObjectURL(blob);
            a.download = `youtube-thumbnail-${quality}.jpg`;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
        } catch (error) {
            console.error("Error downloading image:", error);
        }
    }

    async function handleSearch() {
        const input = document.getElementById('searchInput');
        const resultsContainer = document.getElementById('results');
        const url = input.value.trim();

        if (!url) {
            alert('Please enter a YouTube video URL');
            return;
        }

        const videoId = extractVideoId(url);
        if (!videoId) {
            alert('Invalid YouTube URL. Please check the URL and try again.');
            return;
        }

        resultsContainer.innerHTML = '<p>Loading thumbnails...</p>';

        try {
            const thumbnails = getThumbnailUrls(videoId);
            const qualityOrder = ["max", "hq", "mq", "sd", "default"];
            let availableThumbnails = {};

            for (let quality of qualityOrder) {
                if (await checkImageExists(thumbnails[quality])) {
                    availableThumbnails[quality] = thumbnails[quality];
                }
            }

            if (Object.keys(availableThumbnails).length === 0) {
                resultsContainer.innerHTML = '<p>No thumbnails found for this video.</p>';
                return;
            }

            let thumbnailHtml = `<div class="thumbnail-card">`;
            let bestQuality = Object.keys(availableThumbnails)[0];
            thumbnailHtml += `
                <img src="${availableThumbnails[bestQuality]}" alt="YouTube Thumbnail">
                <div class="download-options">
            `;

            for (let quality in availableThumbnails) {
                thumbnailHtml += `
                    <button class="download-btn" onclick="downloadImage('${availableThumbnails[quality]}', '${quality}')">
                        Download ${quality.toUpperCase()} Resolution
                    </button>
                `;
            }

            thumbnailHtml += `</div></div>`;
            resultsContainer.innerHTML = thumbnailHtml;
        } catch (error) {
            console.error('Error:', error);
            resultsContainer.innerHTML = '<p>An error occurred while fetching thumbnails.</p>';
        }
    }

    document.getElementById('searchInput').addEventListener('keypress', (e) => {
        if (e.key === 'Enter') {
            handleSearch();
        }
    });
});
document.addEventListener("DOMContentLoaded", function() {
    const videoList = document.getElementById("video-list");
    
    // Simulated video data (replace with API calls or database fetch)
    const videos = [
        { title: "Amazing Nature", url: "https://www.w3schools.com/html/mov_bbb.mp4" },
        { title: "Tech Review", url: "https://www.w3schools.com/html/movie.mp4" },
        { title: "Gaming Highlights", url: "https://www.w3schools.com/html/mov_bbb.mp4" }
    ];
    
    setTimeout(() => {
        videoList.innerHTML = ""; // Clear skeleton loaders
        
        videos.forEach(video => {
            const videoItem = document.createElement("div");
            videoItem.classList.add("bg-white", "p-2", "rounded", "shadow");
            videoItem.innerHTML = `
                <video src="${video.url}" controls class="w-full rounded"></video>
                <p class="mt-2 font-semibold">${video.title}</p>
            `;
            videoList.appendChild(videoItem);
        });
    }, 2000); // Simulating load time
});
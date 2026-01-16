const videoElement = document.getElementById('camera-view');
const virtualProduct = document.getElementById('virtual-product');
const productSelect = document.getElementById('product-select');
const tryOnButton = document.getElementById('try-on-button');

const updateProductStyle = () => {
    const videoWidth = videoElement.videoWidth;
    const videoHeight = videoElement.videoHeight;
    
    // Example position and size adjustments
    const productType = productSelect.value;
    switch (productType) {
        case 'hat':
            virtualProduct.style.width = `${videoWidth / 3}px`; // Adjust size as needed
            virtualProduct.style.height = `${videoHeight / 4}px`;
            virtualProduct.style.left = `${(videoWidth / 2) - (parseInt(virtualProduct.style.width) / 2)}px`;
            virtualProduct.style.top = `${videoHeight / 6}px`; // Adjust as needed
            break;
        case 'glasses':
            virtualProduct.style.width = `${videoWidth / 2}px`; // Adjust size as needed
            virtualProduct.style.height = `${videoHeight / 6}px`;
            virtualProduct.style.left = `${(videoWidth / 2) - (parseInt(virtualProduct.style.width) / 2)}px`;
            virtualProduct.style.top = `${videoHeight / 3}px`; // Adjust as needed
            break;
        case 'shirt':
            virtualProduct.style.width = `${videoWidth / 2}px`; // Adjust size as needed
            virtualProduct.style.height = `${videoHeight / 2}px`;
            virtualProduct.style.left = `${(videoWidth / 2) - (parseInt(virtualProduct.style.width) / 2)}px`;
            virtualProduct.style.top = `${videoHeight / 2}px`; // Adjust as needed
            break;
        case 'shoes':
            // Adjust this case based on requirements or skip for face-based tracking
            break;
    }
};

tryOnButton.addEventListener('click', () => {
    const selectedProduct = productSelect.value;
    virtualProduct.src = `${selectedProduct}.png`;
    virtualProduct.style.display = 'block';
    updateProductStyle(); // Update style after changing source
});

navigator.mediaDevices.getUserMedia({ video: true })
    .then(stream => {
        videoElement.srcObject = stream;
        videoElement.onloadedmetadata = () => {
            // Ensure to update the style once video metadata is loaded
            updateProductStyle();
        };
    })
    .catch(error => {
        console.error('Error accessing webcam:', error);
    });
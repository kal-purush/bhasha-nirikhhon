document.addEventListener('DOMContentLoaded', () => {
    const dropZone = document.getElementById('dropZone');
    const fileInput = document.getElementById('fileInput');
    const fileList = document.getElementById('fileList');
    const convertBtn = document.getElementById('convertBtn');

    // Supported formats
    const SUPPORTED_FORMATS = ['.jpg', '.jpeg', '.png', '.bmp', '.tiff', '.heic', '.webp'];
    const MAX_FILE_SIZE = 10 * 1024 * 1024; // 10MB

    let files = [];

    // Drag and drop handlers
    dropZone.addEventListener('dragover', (e) => {
        e.preventDefault();
        dropZone.classList.add('drag-over');
    });

    dropZone.addEventListener('dragleave', () => {
        dropZone.classList.remove('drag-over');
    });

    dropZone.addEventListener('drop', (e) => {
        e.preventDefault();
        dropZone.classList.remove('drag-over');
        handleFiles(e.dataTransfer.files);
    });

    // Click to upload
    dropZone.addEventListener('click', () => {
        fileInput.click();
    });

    fileInput.addEventListener('change', (e) => {
        handleFiles(e.target.files);
    });

    function handleFiles(fileList) {
        files = Array.from(fileList).filter(file => {
            const ext = '.' + file.name.split('.').pop().toLowerCase();
            const isValid = SUPPORTED_FORMATS.includes(ext) && file.size <= MAX_FILE_SIZE;

            if (!isValid) {
                showError(`File "${file.name}" is not supported or too large (max 10MB)`);
            }

            return isValid;
        });

        updateFileList();
        convertBtn.style.display = files.length ? 'inline-flex' : 'none';
    }

    function updateFileList() {
        fileList.innerHTML = '';

        files.forEach(file => {
            const fileItem = document.createElement('div');
            fileItem.className = 'file-item';
            fileItem.innerHTML = `
                <svg class="file-icon" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16l4.586-4.586a2 2 0 012.828 0L16 16m-2-2l1.586-1.586a2 2 0 012.828 0L20 14m-6-6h.01M6 20h12a2 2 0 002-2V6a2 2 0 00-2-2H6a2 2 0 00-2 2v12a2 2 0 002 2z"/>
                </svg>
                <div class="file-info">
                    <div class="file-name">${file.name}</div>
                    <div class="file-size">${formatFileSize(file.size)}</div>
                </div>
                <div class="progress">
                    <div class="progress-bar" style="width: 0%"></div>
                </div>
                <div class="status"></div>
            `;
            fileList.appendChild(fileItem);
        });
    }

    function formatFileSize(bytes) {
        if (bytes === 0) return '0 Bytes';
        const k = 1024;
        const sizes = ['Bytes', 'KB', 'MB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }

    function showError(message) {
        const alert = document.createElement('div');
        alert.className = 'alert alert-error';
        alert.textContent = message;
        fileList.insertBefore(alert, fileList.firstChild);

        setTimeout(() => {
            alert.remove();
        }, 5000);
    }

    // Handle form submission
    convertBtn.addEventListener('click', async () => {
        if (!files.length) return;

        convertBtn.disabled = true;
        const formData = new FormData();

        files.forEach(file => {
            formData.append('files', file);
        });

        try {
            const response = await fetch('/', {
                method: 'POST',
                body: formData
            });

            if (response.ok) {
                // The server will redirect to success.html
                document.documentElement.innerHTML = await response.text();
            } else {
                const error = await response.text();
                showError(error);
                convertBtn.disabled = false;
            }
        } catch (error) {
            showError('An error occurred during conversion');
            convertBtn.disabled = false;
        }
    });
});
if (document.querySelector('#drop-zone')) {
    const dropZone = document.getElementById("drop-zone");
    const fileInput = document.getElementById('file-input')
   
    function handleDrop(e) {
        e.preventDefault();
        dropZone.classList.remove("hover");

        const files = e.dataTransfer.files;
        if (files.length) {
            document.getElementById("file-input").files = files;
            updateThumbnail(files[0]);
        }
    }

    function handleDragOver(e) {
        e.preventDefault();
        dropZone.classList.add("hover");
    }

    function handleDragLeave() {
        dropZone.classList.remove("hover");
    }

    function updateThumbnail(file) {
        const thumbnail = document.createElement("div");
        thumbnail.innerHTML = file.name;
        document.querySelector('[for="drop-zone"]').appendChild(thumbnail);
    }

    dropZone.addEventListener("dragover", handleDragOver);
    dropZone.addEventListener("dragleave", handleDragLeave);
    dropZone.addEventListener("drop", handleDrop);

    document.getElementById("file-input").addEventListener("change", function() {
        updateThumbnail(this.files[0]);
    });

    dropZone.addEventListener('click', () => {
        fileInput.click();
    });
}
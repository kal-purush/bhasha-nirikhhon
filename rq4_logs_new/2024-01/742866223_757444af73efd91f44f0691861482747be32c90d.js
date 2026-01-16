let files = [];
const fileWrapper = document.getElementById("uploadedfiles");
const text = document.getElementById("area-text");
const fileInput = document.getElementById("fileInput");
function uploadFiles(event) {
  if (event) {
    event.preventDefault();
    files.push(...event.dataTransfer.files);
  } else {
    files.push(...fileInput.files);
  }
  for (let i = 0; i < files.length; i++) {
    const reader = new FileReader();
    reader.onload = function (e) {
      fileWrapper.innerHTML += `
      <div class="thumbnail-bg"><img src="${e.target.result}" class="thumbnail"/><span class="remove-text">Remove</span></div>`;
    };
    reader.readAsDataURL(files[i]);
  }
  if (files) {
    text.remove();
  }
}

function openFiles() {
  fileInput.click();
}
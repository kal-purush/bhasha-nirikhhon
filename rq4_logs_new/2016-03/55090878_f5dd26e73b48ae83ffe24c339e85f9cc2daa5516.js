function init() {
    var loader = document.querySelector('#upload');
    loader.addEventListener("change", previewFile);

    var img = document.querySelector('#pickerimg');
    img.addEventListener("mousemove", updateMove);
    img.addEventListener("click", updateClick);
}

function previewFile() {
    var preview = document.querySelector('#pickerimg');
    var file    = document.querySelector('#upload').files[0];
    var reader  = new FileReader();
    
    reader.addEventListener("load", function () {
        var img = new Image();
        img.onload = function(){
            preview.width = img.width;
            preview.height = img.height;
            preview.getContext('2d').drawImage(img,0,0);
        }
        img.src = reader.result;
    }, false);
    
    if (file) {
	reader.readAsDataURL(file);
    }
}

function updateMove(e) {
    updateColour(e,"cucol");
}

function updateClick(e) {
    updateColour(e,"clcol");
}

function updateColour(e,id) {
    var elt = document.getElementById(id);
    var cvs = document.getElementById('pickerimg');
    var pixelData = cvs.getContext('2d').getImageData(e.offsetX, e.offsetY, 1, 1).data;
    elt.innerHTML = `R: ${pixelData[0]} G: ${pixelData[1]} B: ${pixelData[2]} A: ${pixelData[3]}`;
}

window.onload = init;
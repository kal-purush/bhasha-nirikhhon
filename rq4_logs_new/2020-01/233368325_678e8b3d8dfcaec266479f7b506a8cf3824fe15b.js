var imageString = null;
const NO_IMAGE_WARNING_STRING  = "No image given!";

function debug_alert(info) {
    alert(info);
    console.log(info);
}

function encodeImageFileAsURL() {
    var filesSelected = document.getElementById("imageInput").files;
    if (filesSelected.length > 0) {
      var fileToLoad = filesSelected[0];

      var fileReader = new FileReader();

      fileReader.onload = function(fileLoadedEvent) {
        var srcData = fileLoadedEvent.target.result; // <--- data: base64

        var newImage = document.createElement('img');
        newImage.src = srcData;

        document.getElementById("imageTest").innerHTML = newImage.outerHTML;
        imageString = document.getElementById("imageTest").innerHTML
        imageString = imageString.replace('"<img src="data:image/png;base64,"', '');
        imageString = imageString.replace('"<img src="data:image/png;base64,"', '');
        imageString = imageString.slice(0, -1);
        debug_alert("Converted Base64 version is " + imageString);
      }
      fileReader.readAsDataURL(fileToLoad);
    }
  }

function submitImages() {
    console.log("Hi from submitImages");
    if (imageString == null) {
        debug_alert(NO_IMAGE_WARNING_STRING);
        return;
    }
    var imageScaleInput = document.getElementById("imageScaleInput").value;
    document.getElementById("imageScaleInput").value = 0;
    document.getElementById("imageScaleTest").innerHTML = imageScaleInput;
}

function analyzeImage() {
    if (window.XMLHttpRequest) {
        // code for IE7+, Firefox, Chrome, Opera, Safari
        xmlhttp = new XMLHttpRequest();
    } else {
        // code for IE6, IE5
        xmlhttp = new ActiveXObject("Microsoft.XMLHTTP");
    }
    xmlhttp.onreadystatechange = function() {
        if (this.readyState == 4 && this.status == 200) {
            console.log(this.responseText);
            displayValues(this.responseText);
        }
    };
    xmlhttp.open("GET","./analyze-image");
    xmlhttp.setRequestHeader('X-Requested-With', 'XMLHttpRequest');
    xmlhttp.send();
}

function displayValues(jsonResult) {}
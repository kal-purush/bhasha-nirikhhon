function loadPage() {
    var url = document.getElementById("urlInput").value;
    var iframe = document.getElementById("browserFrame");
    iframe.src = url;
}
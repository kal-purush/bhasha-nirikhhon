function load(url, callback) {
    let xhr = new XMLHttpRequest()
    xhr.onreadystatechange = function() {
        if (this.readyState == 4 && this.status == 200) {
            callback(this)
        }
    }
    xhr.open("GET", url, true)
    xhr.send()
}
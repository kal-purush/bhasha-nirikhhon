console.log("hola mundo")
fetch("cv.json")
.then(resp => resp.json())
.then(data => console.log(data))
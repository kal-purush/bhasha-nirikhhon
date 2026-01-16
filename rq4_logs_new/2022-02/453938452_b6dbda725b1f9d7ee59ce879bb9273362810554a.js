function generate(){
  let xmlhttp = new XMLHttpRequest();
  xmlhttp.onreadystatechange = function() {
    if (this.readyState == 4 && this.status == 200){
      let response = JSON.parse(this.responseText);
      console.log(response);
      document.getElementById("text-gen").innerHTML = response["text"];
    }
  };
  xmlhttp.open("POST", "/sequence", true);
  xmlhttp.setRequestHeader("Content-type", "application/x-www-form-urlencoded");
  xmlhttp.send("");
}

generate_btn = document.getElementById("generate-btn");
console.log(generate_btn);
generate_btn.addEventListener("click", generate);
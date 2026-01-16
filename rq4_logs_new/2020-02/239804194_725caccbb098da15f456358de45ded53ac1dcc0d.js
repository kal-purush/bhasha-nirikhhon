function response(event){
    event.preventDefault();   
    var name=validateFormData(pattern, 'name',form);
    var email=validateFormData(pattern, 'email',form);  

    if(name==true && email==true){
        var formData=createFormObject(["name","email","subject","message"],form);
        var response=convertToJson(formData);
        var xhttp = new XMLHttpRequest();
        xhttp.onreadystatechange = function() {
            if (this.readyState == 4 && this.status == 200){
                for(var i=0; i<form1.length; i++){
                    form1[i].value="";
                    changeBorderColor(form1[i], "transparent");
                }
                alert(this.responseText);
                
            }
        };

        xhttp.open("POST", "php/send_message.php", true);
        xhttp.setRequestHeader("Content-type", "application/x-www-form-urlencoded");
        xhttp.send("response="+response);
        }
}
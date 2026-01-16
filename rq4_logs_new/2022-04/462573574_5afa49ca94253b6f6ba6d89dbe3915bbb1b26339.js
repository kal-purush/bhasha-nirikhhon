$(document).ready(function() {
    class User {
        constructor (id, fname, lname, email, phone, nickname){
            this.id = id;
            this.fname = fname;
            this.lname = lname;
            this.email = email;
            this.phone = phone;
            this.nickname = nickname;
   
        }
    }

    
    function autofill(){
        var user = new User();
        $.getJSON("apis/idtousername.php?id=", function(data) {
            

        });
        console.log("cheked");
        $("#fname").val("name");
        $("#lname").val("name");
        $("#email").val("name");
        $("#phone").val("name");
    }
    document.getElementById("use-profile").addEventListener("click", function () {
        autofill();
    });

});
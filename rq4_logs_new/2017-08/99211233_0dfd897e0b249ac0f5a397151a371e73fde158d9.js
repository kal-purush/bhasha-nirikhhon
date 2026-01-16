var username = "akshay";
//class for get and set
var getsetclass = (function () {
    function getsetclass(uname) {
        this.uname = uname;
    }
    Object.defineProperty(getsetclass.prototype, "username", {
        get: function () {
            if (this.uname == username) {
                return username + " user exists ..!!";
            }
            else {
                return "User doesnot exists..>!!";
            }
        },
        set: function (newname) {
            if (this.uname == username) {
                console.log(newname);
                this.uname == newname;
            }
            else {
                alert("Wrong User Name..!!");
            }
        },
        enumerable: true,
        configurable: true
    });
    return getsetclass;
}());
function myFunction(id) {
    var uname = document.getElementById("username").value;
    if (uname == "") {
        alert("Enter The username First..!!");
    }
    else {
        //create new object pf getset class
        var user = new getsetclass(uname);
        if (id == "get") {
            //call the get of class
            document.getElementById("res").innerHTML = user.username;
        }
        else {
            //call the set of class
            user.username = uname;
            console.log(user);
            //document.getElementById("res").innerHTML = "";
        }
    }
}
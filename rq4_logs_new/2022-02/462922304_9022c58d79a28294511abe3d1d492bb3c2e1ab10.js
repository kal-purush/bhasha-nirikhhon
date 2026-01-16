var ObjClients =[
    {
        username: "June",
        email: "liuyijun07@gmail.com",
        password: "1013",
        AccountBalance: 1013999
    },

    {
        username: "Kevin",
        email: "kevin-fang@hotmail.ca",
        password: "0821",
        AccountBalance: 88
    },

    {
        username: "Stanley",
        email: "zhih0040@gmail.com",
        password: "0222",
        AccountBalance: 1.05
    }

];

console.log(ObjClients[2].password)

function myFunction(){
    var username = document.getElementById("username").value
    var password = document.getElementById("password").value

for (let i=0; i<ObjClients.length; i++){
    if (username == ObjClients[i].username && password == ObjClients[i].password){
        document.getElementById("funText").innerHTML = "Hi " + username + ", Welcome to Bank of Kevin, please select the options above.";
        $("#factButton").on("click", function(){
        document.getElementById("accountText").innerHTML = "Your current balance is $" + ObjClients[i].AccountBalance;
        })
        $("#cashWithdraw").on("click", function(){
        document.getElementById("withdrawText").innerHtml = "How much do you need to withdraw?";
        })
        return
    } 
}
document.getElementById("funText").innerHTML = "Wrong Credentials, please enter again.";


}






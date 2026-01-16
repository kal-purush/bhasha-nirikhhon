function compute()
{
//Getting data from HTML file//
    var principal = document.getElementById("principal").value;
    var rate = document.getElementById("rate").value;
    var years = document.getElementById("years").value;
//Calculation logic//
    var interest = principal * years * rate/100;
    var amount = parseFloat(interest);
//Converting no of years to actual year in the future//
    var year = new Date().getFullYear()+parseInt(years);
//Making sure the user has input some Positive Number 
    if (principal<= 0)
    {
        //show alert if input isn't there or if input is <=0
        window.alert("Enter a positive Number first")
    }
    else
    {
        //Valid Inputs
        document.getElementById("result").innerHTML = "If you deposit "+principal+",\<br\>at an interest rate of "+rate+"%\<br\>You will receive an amount of "+amount+"/-\<br\>In the year "+year+"\<br\>";
    }
}

function updateRate()
{
    //Get the value of rate from the range
    var rateval = document.getElementById("rate").value;
    //Put the value instead of 10.25%
    document.getElementById("rate_val").innerHTML = rateval+ "%";
}
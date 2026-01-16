function view(num)
{
    document.getElementById("result").value+=num
}

function compute()
{
    var x = document.getElementById("result").value
    var z = eval(x)
    document.getElementById("result").value = z
}

function clr()
{
    document.getElementById("result").value = ""
}
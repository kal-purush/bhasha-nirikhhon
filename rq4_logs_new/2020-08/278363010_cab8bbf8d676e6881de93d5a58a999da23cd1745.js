let num = 0;
function input(){
    let input = document.getElementById("Input_Bar").value;
    num = input;
}
function GooGoodan(){
    for(var i=1;i<=9;i++)
    {
        document.write(num + "x" + i + "=" + num*i + "<br>");
    }
}
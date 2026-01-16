$(document).ready(()=>{
    query("Accra");
    $("#register-form").submit((e)=>{
        $("#loader").attr("src","images/loader.gif");
        let input = $(".query").val();
        let city = copydata(input);
        e.preventDefault();
        setTimeout(() => {
            query(city);
        }, 1000);
    });
});
function copydata(input) {
    if (input.length ==0) {
        input = "Accra";
        return input;
    } if (input.length >0) {
        let string = input.toLowerCase();
        let first = input[0].toUpperCase();
        let name = first+string.slice(1);
        return name;
    } 
}
function getdata(json){
    let description = json.weather[0].description;
    let temperature = Math.floor(json.main.temp);
    let url = "https://openweathermap.org/img/w/";
    let icon = json.weather[0].icon+".png";
    let imgpath = url+icon;
    return "<h2 style='padding-bottom:10px;'>"+json.name+", <span style='font-size:28px;'>"+json.sys.country+"</span></h2>"+
           "<h1 style='font-weight:500;font-size:70px;'>"+ temperature +"<sup style='font-size:50px;font-weight:300;'>&deg;C</sup></h1>"+
           "<img width='90' height='90' src='"+imgpath+"'>"+
           "<h4 style='color:yellowgreen;'>Atm.Pressure:"+ json.main.pressure+" hPa" + " Humidity: "+ json.main.humidity +" %" +"<h4>"+
           "<h4 style='padding:5px;font-weight:200;'>"+ description.toUpperCase();  +"</h4>"
           ;
}
function query(city) {
    if (!navigator.onLine) {
        $(".displayer").html("<h3>Your device is currently offline. Please connect to the internet to continue using the weather</h3>");
        $(".query").blur();
        $(".query").val("");
        $("#loader").attr("src","");
    } if (navigator.onLine) {
        $.ajax({
            url:"https://api.openweathermap.org/data/2.5/weather?q="+city+"&units=metric"+"&APPID=6e268533db98499c8b48997b3e410624",
            type:"GET",
            dataType:"jsonp",
            success:function(data)
            { 
                $(".displayer").html(getdata(data));
                $(".query").blur();
                $(".query").val("");
                $("#loader").attr("src","");
            }
        });
    }     
}
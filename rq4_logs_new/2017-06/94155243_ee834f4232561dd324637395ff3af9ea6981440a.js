$(document).ready(function(){
  
  //group of background pics
  var back = {
    "01d":"http://snehaschoice.com/wp-content/uploads/2016/03/clear-sky-on-a-sunny-day-1255-1366x768.jpg",
    "01n":"http://www.tudesign.cn/Uploads/Picture/2015-09-19/55fcdf22ebab0.jpg",
    "02d":"https://3.bp.blogspot.com/-cZmIORs9IVM/UPb6Q8oc33I/AAAAAAAACnU/1vAfWwkA4K0/s1600/bright-blue-sky-with-a-few-tiny-white-clouds.jpg",
    "02n":"http://www.drahtphotography.com/wp-content/uploads/2015/05/sampleIMG_3642.jpg",
    "03d":"http://cloud-maven.com/wp-content/uploads/2013/12/DSC_0506-1024x682.jpg",
    "03n":"https://trentsthoughts.files.wordpress.com/2011/05/night_sky.jpg",
    "04d":"http://www.quotemaster.org/images/55/55177ce7bd350616d00f1dba9b4f2ec6.jpg",
    "04n":"http://wallpapers-hd-wide.com/wp-content/uploads/2016/01/Beautiful-cloudy-night-full-moon-moonlight-1920x1080.jpg",
    "09d":"https://upload.wikimedia.org/wikipedia/commons/thumb/7/75/A_rainy_day_at_the_Charles_River_Esplanade%2C_in_Boston.jpg/1280px-A_rainy_day_at_the_Charles_River_Esplanade%2C_in_Boston.jpg",
    "09n":"https://i.imgur.com/j1DWGOB.jpg",
    "10d":"http://wallpaper-gallery.net/images/rainy-day-wallpapers/rainy-day-wallpapers-11.jpg",
    "10n":"https://s-media-cache-ak0.pinimg.com/originals/e3/80/1f/e3801f3c54ca684f04c621caa0fcf9c9.jpg",
    "11d":"http://farmersalmanac.com/wp-content/uploads/2015/06/Thunderstorm-5best.jpg",
    "11n":"http://farmersalmanac.com/wp-content/uploads/2015/06/Thunderstorm-5best.jpg",
    "13d":"https://4.bp.blogspot.com/-Yz3x7hU91KQ/Vp3f40OsUPI/AAAAAAAACuw/QiIScM1IX-c/s1600/Bahaya%2BMemakan%2BSalju%2521%2BAlasan%2BMengapa%2BKita%2BTidak%2BBoleh%2BMemakannya.jpg",
    "13n":"https://4.bp.blogspot.com/-Yz3x7hU91KQ/Vp3f40OsUPI/AAAAAAAACuw/QiIScM1IX-c/s1600/Bahaya%2BMemakan%2BSalju%2521%2BAlasan%2BMengapa%2BKita%2BTidak%2BBoleh%2BMemakannya.jpg",
    "50d":"https://vignette1.wikia.nocookie.net/demigodshaven/images/f/f5/Mist.jpg/revision/latest?cb=20110102163040",
    "50n":"https://vignette1.wikia.nocookie.net/demigodshaven/images/f/f5/Mist.jpg/revision/latest?cb=20110102163040",    
  }
  
  //get location
  var city="";
  var countryCode="";
  $.getJSON("http://ip-api.com/json",function(json){
    city=json.city;
    countryCode=json.countryCode;
    $("#location").html(city+", "+countryCode);
    //get the weather
    $.getJSON("http://api.openweathermap.org/data/2.5/weather?q="+city+","+countryCode+"&units=metric&appid=1805eea8d028a0b284a3378ca5eb037d", function(json1){
      $("#summary").html(json1.weather[0]["description"]);
      temp=json1.main.temp;
      $("#temperature").html(temp+" °C");
      var iconURL="http://openweathermap.org/img/w/"+json1.weather[0]["icon"]+".png";
      $("#icon").attr("src",iconURL);
      var backURL="url("+back[json1.weather[0]["icon"]]+")";
      $("body").css("background-image",backURL);
      
      
      //make everthing apear
      $("p,img").css("display","block");
      $(".fa").css("display","none");
    });
  });
  
  //change units
  var unit=0;
  $("#temperature").on("click", function(){
    unit++;
    if (unit%2!=0){
      tempF=temp*9/5+32;
      $("#temperature").html(tempF+" F");
    }else{
      $("#temperature").html(temp+" °C");
    }
  });
  
});
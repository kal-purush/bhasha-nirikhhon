 var divlength = document.querySelectorAll('.song').length;
 for (var i = 0; i < divlength; i++) {
     var demo = document.querySelectorAll('.song')[i].addEventListener('click', playsong);
     var demo = document.querySelectorAll('.song')[i].addEventListener('dblclick', pausesong);

 }

 var song1 = new Audio();
 song1.src = "music/Shivrai chakravarti full song by chatrapati shivaj(MP3_128K).mp3";

 var song2 = new Audio();
 song2.src = "music/Vajrabahu Mahabahu (Kondaji Theme)(songclub.in).mp3";

 var song3 = new Audio();
 song3.src = "music/Rani Phadakti Lakho Zende _ Fatteshikast _ Chinmay(MP3_128K).mp3";

 var song4 = new Audio();
 song4.src = "music/SHIV SENA GEET(MP3_128K).mp3";

 var song5 = new Audio();
 song5.src = "music/Tanhaji Songs Jukebox _ Tanhaji Movie all Songs(MP3_70K).mp3";



 function playsong() {
     var songId = this.innerHTML;

     switch (songId) {
         case "a":
             song1.play();
             break;

         case "b":
             song2.play();
             break;

         case "c":
             song3.play();
             break;

         case "d":
             song4.play();
             break;

         case "e":
             song5.play();
             break;

         default:
             alert("wrong click!!!")

     }
 }

 function pausesong() {
     var songId = this.innerHTML;
     switch (songId) {
         case "a":
             song1.pause();
             break;

         case "b":
             song2.pause();
             break;

         case "c":
             song3.pause();
             break;

         case "d":
             song4.pause();
             break;

         case "e":
             song5.pause();
             break;

         default:
             alert("wrong click!!!")

     }
 }
 $(document).ready(function() {
     var response = '';
     $.ajax({
         type: "GET",
         url: "Records.php",
         async: false,
         success: function(text) {
             response = text;
         }
     });

     alert(response);
 });
var images= 
[
    "me.JPG",
    "acha.JPG",
    "amma.JPG",
    "lachuchechi.JPG"
];

var names = 
[
    "Vishnu Sundaram",
    "Rahul Sundaram",
    "Rajini Rahul",
    "Lakshmi Rahul"
]
  var i = 0;
  function update() 
  {
    i++;
    var people = 4;
    if (i > people)
      {
        i=0;
      }
    
    document.getElementById("img1").src = images [i];
    document.getElementById("names_of_members").innerHTML = names [i];
  }
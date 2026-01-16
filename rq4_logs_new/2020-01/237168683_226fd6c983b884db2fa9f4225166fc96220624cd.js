var animals = ["dog", "fish", "cat", "shark", "cat", "dog"];
for (i = 0; i < animals.length; i++) { 
  if (animals[i]=="cat"){
    document.write("I am a cat<br>");
  }
  else if (animals[i]=="dog"){
    document.write("borf borf<br>");
  }
  else{
    document.write("I am an animal<br>");
  }
}
//funtion for request gists
function requestGists(){
  //found this httprequest from the discussions in class and from week 4 readings. 
  var httpReq = new XMLHttpRequest;
  //if 4 pages AND (second loop?) if the status is 200
  httpReq.onreadystatechange = function () {
  if (this.readyState === 4) {
      if (this.status === 200) {

    var arrayofGists = JSON.parse(httpReq.responseText);
        console.log(arrayofGists[0].url);
        createList (arrayofGists);
  }
      //need an error message
       else {
         //error message
        alert('Error. There was a problem submitting your request.');
    }
    }
  }
  //get the information from github using GET, not POST
  httpReq.open('GET', '//api.github.com/gists/public');
  httpReq.send();
 }
 //funtcion for creating a list named createList. 
 function createList(arrayofGists) {
   var gistElements = document.getElementById( "results");

   //loop and add list and id. use li in document.createELement to make it a list
   //don't label it list. it will get confusing. or li. 
     for (var i = 0; i <arrayofGists.length; i++){
     var gistlist = document.createElement("li");
     gistlist.id = i + "n";


    //if false add new url/no description
      //if '' same as above
      //use here: Object.prototype.hasOwnProperty()
      //from https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object/hasOwnProperty
  if(arrayofGists[i].hasOwnProperty.call(arrayofGists[i],'description') ===  false){
      gistlist.innerHTML = '<a href='+arrayofGists[i].url + '>' + 'No Description' + '</a>';
    }

    //can't figure out the ""! 
    else if(arrayofGists[i].hasOwnProperty.call(arrayofGists[i],'description') ===  ""){
      gistlist.innerHTML = '<a href='+arrayofGists[i].url + '>' + 'No Description' + '</a>';
    }

    //give arrayofgists.description if passes other 2 statements
    else{
      gistlist.innerHTML = '<a href= ' + arrayofGists[i].url+'>' + arrayofGists[i].description+'</a>';
    }

    //create favorites button
  this.favButton = document.createElement('button'); 
    this.favButton.innerHTML = "Save to Favorites";

    //function for favorites - to store favorite in local storage
    this.favButton.onclick = function () {
  //par = favButton.parentNode;
    //localStorage.setItem(favoriteButton.parentNode.id, favoriteButton.childNode.innerHTML);
      localStorage.setItem(favButton.parentNode.id, favButton.parentNode.innerHTML);

    //   gistlist.appendChild(this.favButton);
    // gistElements.appendChild(gistlist);
  }

  gistlist.appendChild(this.favButton);
     gistElements.appendChild(gistlist);
     console.log(favButton);
     }
 
}
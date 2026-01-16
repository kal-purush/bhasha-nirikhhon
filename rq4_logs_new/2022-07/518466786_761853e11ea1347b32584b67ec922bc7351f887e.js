/* function abbrevName(firstName, lastName){
    const initials = firstName[0].toUpperCase() + "." + lastName[0].toUpperCase()
    console.log (initials)
    }
  abbrevName('alexander', 'kovalenko')


  function past(h, m, s){
    let time = h*3600000 + m*60000 + s*1000
    console.log(time)
  }

 past(23, 59, 59);

 function openOrSenior(data){
  const emptyArray = []
  for(i = 0; i < data.length; i++) {
    if(data[i][0] >= 55 && data[i][1] > 7) {
      emptyArray.push('Senior')
    }
    else {
      emptyArray.push('Open')
    }
  }
  return emptyArray
}

console.log(openOrSenior([[25, 8]])) */

let map;

function initMap() {
  const myLatLng = { lat: 47.214879096350394, lng: 39.70497243644624 };
  const map = new google.maps.Map(document.getElementById("map"), {
    zoom: 17,
    center: myLatLng,
  });

  new google.maps.Marker({
    position: myLatLng,
    map,
    title: "ул. Тургеневская, 10/6",
  });
}

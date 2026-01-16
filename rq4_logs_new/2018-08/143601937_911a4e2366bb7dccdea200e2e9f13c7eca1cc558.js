function red() {
  var url = window.location.href;

  //url = url.replace("https://dbdemo-b1622.firebaseapp.com/","");

  var req = new Request("https://us-central1-dbdemo-b1622.cloudfunctions.net/geturl", {
    method: "POST",
    headers: new Headers({"Content-Type": "application/json"}),
    body: JSON.stringify({url: url})
  });

  fetch(req).then( (res) => {
    res.json().then( (data) => {
      window.location = data.url;
      console.log(data);
    });
  });

  //alert("got the id: " + url);

  console.log(url + " gotcha ! ");
}
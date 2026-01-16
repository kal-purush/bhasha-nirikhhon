fetch('testpp.json')
    .then(function (response) {
        return response.json()
    })
    .then(function(data) {
        appenData(data)
    })
    .catch(function (err) {
        console.log('error: ' + err)
    })

    function appenData(data){
        var  mainContainer = document.getElementById("myData");
        for (var i = 0; i < data.length; i++) {
            var div = document.createElement("div");
            div.innerHTML = "ID : " + data[i].id + " " + data[i].firstName + ' ' + data[i].lastName;
            mainContainer.appendChild(div);
        }
    }
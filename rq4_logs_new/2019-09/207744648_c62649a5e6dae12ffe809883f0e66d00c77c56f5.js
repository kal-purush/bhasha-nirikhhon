import * as data from './RWC2019Predictor.js'

const appbaseRef = Appbase({
	url: 'https://scalr.api.appbase.io',
	app: 'RWC2019PointsTable',
	credentials: 'NS8mA0TkJ:2f5b1e35-d02b-4807-9c6a-a751390998f9',
});

let array = [];
const top = document.getElementById("top");

window.onload = function() {
    appbaseRef.search({
        type: "doc",
            body: {
                size : 100,
                query: {
                    match_all: {},
                },
            }
      }).then(function(response) {
        console.log(response);
        array = response.hits.hits;
        let row, cell = [];
        let subRow = 0;
        for(let k=0; k<array.length; k++){
            if(array[k]._id != "AWzYS72NuDNiuUUj251k" && array[k]._source.pointssubmitted == true){
                let team = 0;
                subRow++;
                row = top.insertRow(subRow);
                for(let r=0; r<21; r++){
                    cell[r] = row.insertCell(r);
                }
                cell[0].innerHTML = array[k]._source.name;
                for(let prop in array[k]._source){
                    team++;
                    if(array[k]._source[prop] >= 0 && prop!="pointssubmitted" && prop!="score"){
                        cell[team].innerHTML = array[k]._source[prop];
                        if(array[k]._source[prop]>0 && subRow%2){
                            cell[team].classList.add("JPN");
                        }else if (array[k]._source[prop]>0 ){
                            cell[team].classList.add("ITA");
                        }
                    }
                }
            }
        }
    }).catch(function(error) {
    alert(error)
    })
};

document.getElementById(`back`).onclick = function() {
    window.open(`index.html`, "_self");
}
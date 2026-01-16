
var input1 = document.getElementById("inputGroupSelect02");


function update() {
    var giris1;
    var giris2;
    guncel();
    console.log(guncel());
}

function guncel(){
    var select = document.getElementById('inputGroupSelect01');
    var option = select.options[select.selectedIndex];

}



function update2() {
    var select2 = document.getElementById('inputGroupSelect02');
    var option2 = select2.options[select2.selectedIndex];
    return(option2.value);
}

console.log(update2());






let dizi = {
     csgo: 1,
     valorant: 3,
     apex: 4,
     zula: 7,
};
var islem = dizi.csgo + dizi.valorant;

console.log(islem);

var course = {
    name: "GRA 2032",
    start: 8,
    end: 10
};
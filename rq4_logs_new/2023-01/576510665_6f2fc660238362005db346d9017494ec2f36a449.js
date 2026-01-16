document.getElementById("kmenu").onkeyup = function(){
    let txtnama = document.getElementById("kmenu").value;
    let txtnama2 = txtnama.toUpperCase();
    document.getElementById("kmenu").value = txtnama2;
};

document.getElementById("global1").onload = function(){
    let today = new Date();
    let dd = String(today.getDate());
    let yy = today.getFullYear();
    let tgl;

    today = dd + yy;
    tgl = document.getElementById('kode').value = "INV" + today + "001";
};

document.getElementById("btn").onclick = function(){
    let menu = document.getElementById('kmenu').value;

    let nama = document.getElementById('nmenu').value;

    combobox();
    
    let jb = document.getElementById('jb').value;

    let pembayaran;
        if(document.getElementById("tn").checked == true){
            pembayaran = "Tunai"};
        if(document.getElementById("ntn").checked == true){
            pembayaran = "Non-Tunai"};

    let harga = document.getElementById('harga').value;
            
    total(jb,harga); 
            
    document.getElementById('data1').innerHTML = menu;
    document.getElementById('data2').innerHTML = nama;
    document.getElementById('data4').innerHTML = jb
    document.getElementById('data7').innerHTML = pembayaran;
}
function combobox(){
    let jenis = document.getElementById('form1').jm.value;
        if(jenis ==  "Makanan"){
            document.getElementById('data3').innerHTML="Makanan";
        }
        if(jenis == "Minuman"){
            document.getElementById('data3').innerHTML="Minuman";
        }
        else if(jenis == "Snack"){
            document.getElementById('data3').innerHTML="Snack";
        }
}
function total(jb,harga){
    if(jb > 10){
        document.getElementById('data5').innerHTML = "2%"
    }
    if(document.getElementById('data5').value = "2%"){
        let first = (jb*harga)*2/100;
        let sc = (jb*harga)-first;
        document.getElementById('data6').innerHTML = sc;
    }
}
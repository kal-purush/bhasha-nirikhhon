
let prom = prompt("Masukkan jumlah data barang :")
let array = [];
let jumray = [];

for (i = 0; i <= prom ; i++) {
    let barang = prompt("Masukkan Nama barang ke- " +i)
    array.push(barang);     
    let jumlah = prompt("Masukkan jumlaah barang ke-" +i)
    jumray.push(jumlah)
}


console.table("Nama Barang : ", array, "Jumlaah barang", jumray);
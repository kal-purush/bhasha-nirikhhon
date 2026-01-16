let operand1;
let operand2;
let operand3;
let prom;

function inputOperator() {
  prom = prompt(
    `Pilih Menu:\n 1. Penjumlahan \n 2. Pengurangan \n 3. Pembagian \n 4. Perkalian \n 5. Pangkat 2 \n 6. Akar Pangkat 2 \n 7. Akar pangkat 3`
  );

  switch (prom) {
    case "1":
      masukkanAngka2();
      penjumlahan(operand1, operand2);
      konfirm();
      break;
    case "2":
      masukkanAngka2();
      pengurangan(operand1, operand2);
      konfirm();
      break;
    case "3":
      masukkanAngka2();
      break;
    case "4":
      masukkanAngka2();
      break;
    case "5":
      masukkanAngka1();
      pangkat2(operand3);
      konfirm();
      break;
    case "6":
      masukkanAngka1();
      akar2(operand3);
      konfirm();
      break;
    case "7":
      masukkanAngka1();
      akar3(operand3);
      konfirm();
      break;
    default:
      alert("Masukkan nomor dari opsi di atas!!!");
      konfirm();
  }
}

function masukkanAngka2() {
  operand1 = parseInt(prompt("Masukkan angka pertama: "));
  operand2 = parseInt(prompt("Masukkan angka kedua: "));
}
function masukkanAngka1() {
  operand3 = parseInt(prompt("Masukkan angka: "));
}

function penjumlahan(operand1, operand2) {
  alert(`${operand1} + ${operand2} = ${operand1 + operand2}`);
}

function pengurangan(operand1, operand2) {
  alert(`${operand1} - ${operand2} = ${operand1 - operand2}`);
}

function pangkat2(operand3) {
  alert(`${operand3} * ${operand3} = ${operand3 * operand3}`);
}

function akar2(operand3) {
  alert(`${Math.sqrt(operand3)}`);
}

function akar3(operand3) {
  alert(`${Math.cbrt(operand3)}`);
}

function konfirm() {
  if (confirm("Mau hitung ulang?") == true) {
    inputOperator();
  } else {
    alert("Terimakasih :)");
  }
}

inputOperator();
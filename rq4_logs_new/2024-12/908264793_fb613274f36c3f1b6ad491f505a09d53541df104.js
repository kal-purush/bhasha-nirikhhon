function sumMatrices() {
   
    let a11 = parseInt(document.getElementById("a11").value);
    let a12 = parseInt(document.getElementById("a12").value);
    let a21 = parseInt(document.getElementById("a21").value);
    let a22 = parseInt(document.getElementById("a22").value);


    let b11 = parseInt(document.getElementById("b11").value);
    let b12 = parseInt(document.getElementById("b12").value);
    let b21 = parseInt(document.getElementById("b21").value);
    let b22 = parseInt(document.getElementById("b22").value);

    let r11 = a11 + b11;
    let r12 = a12 + b12;
    let r21 = a21 + b21;
    let r22 = a22 + b22;

    document.getElementById("r11").value = r11;
    document.getElementById("r12").value = r12;
    document.getElementById("r21").value = r21;
    document.getElementById("r22").value = r22;
}
function mulMatrices() {
   
    let a11 = parseInt(document.getElementById("a11").value);
    let a12 = parseInt(document.getElementById("a12").value);
    let a21 = parseInt(document.getElementById("a21").value);
    let a22 = parseInt(document.getElementById("a22").value);


    let b11 = parseInt(document.getElementById("b11").value);
    let b12 = parseInt(document.getElementById("b12").value);
    let b21 = parseInt(document.getElementById("b21").value);
    let b22 = parseInt(document.getElementById("b22").value);

    let r11 = a11*b11 + a12*b21;
    let r12 = a11*b12 + a12*b22;
    let r21 = a21*b11 + a22*b21;
    let r22 = a21*b12 + a22*b22;

    document.getElementById("r11").value = r11;
    document.getElementById("r12").value = r12;
    document.getElementById("r21").value = r21;
    document.getElementById("r22").value = r22;
}
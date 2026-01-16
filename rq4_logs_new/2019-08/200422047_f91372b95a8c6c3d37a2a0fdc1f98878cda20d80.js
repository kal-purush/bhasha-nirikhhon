function bonAppetit(bill, k, b) {
    var billTotal = 0;
    var anna = 0;
    var devolucao = 0;

    for (var i = 0; i < bill.length; i++){
        billTotal += bill[i];
    }
    anna = (billTotal - bill[k])/2;
    devolucao = b - anna;

    if (b == anna) console.log("Bon Appetit");
    else if (b > anna) console.log(devolucao);

}
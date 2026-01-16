function myfunc() {
    var customerNumber = document.getElementById("customerNumber");
    var customerNumberHidden = document.getElementById("customerNumberHidden");
    customerNumberHidden.value = customerNumber.value;
    document.forms[1].submit();

}
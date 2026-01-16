
  const day = new Date().getDay();

  switch (day) {
    case 0:
    document.write("It's Sunday, I have class tomorrow.");
    break;
    case 1:
    document.write("It's Monday, I have three classes today.");
    break;
    case 2:
    document.write("It's Tuesday, I only have one class today.");
    break;
    case 3:
    document.write("It's Wednesday, I have three classes today.");
    break;
    case 4:
    document.write("It's Thursday, I have work but no classes.");
    break;
    case 5:
    document.write("It's Friday, I have work but no classes.");
    break;
    case 6:
    document.write("It's Saturday, I have off today.");
    break;
  }



function add()
{
  var txtNumber = document.getElementById("txtNumber");
  var newNumber = parseInt(txtNumber.value) + 1;
  txtNumber.value = newNumber;
}

function subtract()
{
  var txtNumber = document.getElementById("txtNumber");
  var newNumber = parseInt(txtNumber.value) - 1;
  txtNumber.value = newNumber;
}
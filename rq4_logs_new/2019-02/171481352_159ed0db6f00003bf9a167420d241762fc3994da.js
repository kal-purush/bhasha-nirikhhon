function oddPosition() {
  var myArr= [5 , 1 , 2 , 7 , 4];
  var sum;
  sum=Number(sum);

  for (var i = 0; i < myArr.length; i++) {

    if (i%2==1) {
      sum+=myArr[i];
    }
  }

return sum;

}


document.getElementById('somma').innerHTML=oddPosition();
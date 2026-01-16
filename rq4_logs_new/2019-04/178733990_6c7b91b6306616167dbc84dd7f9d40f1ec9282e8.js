function grow(x){
    var sum = 1;
    for(let i = 0; i < x.length; i++)
    {
      sum = sum * x[i];
    }
    return sum;
  }
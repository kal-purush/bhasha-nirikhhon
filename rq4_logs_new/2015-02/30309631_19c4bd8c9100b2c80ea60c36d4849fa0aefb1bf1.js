function fizzBuzz() {
  for(i = 1; i <= 100; i++) {
    switch (true) {
      case (i % 3 === 0) && (i % 5 == 0): 
        console.log("fizzbuzz"); 
        break;
      case (i % 5 === 0): 
        console.log("buzz");  
        break;
      case (i % 3 ===0): 
        console.log("fizz"); 
        break;
      default: 
        console.log(i);
        break;
    }
  }
}

fizzBuzz();
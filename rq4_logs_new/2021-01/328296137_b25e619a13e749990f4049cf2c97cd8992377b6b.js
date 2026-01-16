// Space complexity of memory

// Follow the big O cheat sheet for more information.

function boo(n){
    for (let i=0; i<n.length; i++){
      console.log('Booo');
    }
  }
  boo([1,2,3,4,5]);// O(1) constante
  
  // ========================
  
  function arrayOfHiNTimes(n){
    let hiArray = [];
    for(let i = 0; i < n; i++){
      hiArray[i] = 'hi';
    }
    return hiArray;
  }
  
  arrayOfHiNTimes(6); // O(n) 
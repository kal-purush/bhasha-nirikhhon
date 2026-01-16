// ------------------1----------------
// Example x = 15438;
// Expected Output : 83451


// let data = [1,5,4,3,8]
// console.log(data.reverse().join(''));

// -----------------2---------------
// Example string : 'webmaster'
// Expected Output : 'abeemrstw'
// Assume punctuation and numbers symbols are not included in the passed string.


// function alphabet(str) {
    
//     return str.split('').sort().join('');
//   }

//   console.log(alphabet("webmaster"));


// --------------------------3---------------------
// Example string : 'the quick brown fox'
// Expected Output : 'The Quick Brown Fox'



// 3. Write a JS function that accepts a string as a parameter and converts the first letter of each word of the string in upper 

function upper(string){
  let array = string.split(' ');
  let newArray = [];
  for(let x = 0; x < array.length; x++){
      newArray.push(array[x].charAt(0).toUpperCase() + array[x].slice(1));
  }return newArray.join(' ');
}
console.log(upper('the quick brown fox'));

// ------------------------------4---------------------

// 4. Write a JavaScript function that accepts a string as a parameter and finds the longest word within the string.
// Example string : 'Web Development Tutorial'
// Expected Output : 'Development'
function findLongestWord(str) {

  var words = str.split(' ');
  
  var maxLength = 0;
  
  var longestWord = '';
  
  
   for (var i = 0; i < words.length; i++) {
  
   if (words[i].length > maxLength) {
  
   maxLength = words[i].length;
  
   longestWord = words[i];
  }
}
  
   return longestWord;
  }
  
  var longest = findLongestWord("Web Development Tutorial");
  console.log(longest);

// ---------------------5------------------
// 5. Write a JavaScript function that checks whether a number is perfect.

let p= (a) =>{
  let n = 0;
  for(let i = 1; i<a; i++)
  {
    if( a % i == 0)
    {
      n = n + i;

    }
  }
  if(a == n)
  {
    console.log("number is perfact");
  }
  else{
    console.log("number is not perfact");
  }
}
p(4);
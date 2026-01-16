//q1
// var a = 123;
// var b; 
// sum = 0;
// var z = a;
// while(a > 0)
// {
// b = a % 10;
// sum = sum * 10 + b;
// a = parseInt(a / 10);
// }
// alert(sum);

// q2

//function that check str is palindrome or not
// function pal(){
//     var x=document.getElementById("a").value;
//     var y="";
//     for (i=x.lenght-1;i>0;i--){
//         y=y+x[i];
//     }
//     if(x===y)
//         document.getElementById("b").value=y+"is a palindrome";
//         else
//         document.getElementById("b").value=y+"is Not a palindrome";
        
    
// }
//q3
let possibleCombinations = (str) =>{
    let combinations = [];
      for(let i = 0 ;i < str.length; i++)
    {
        for(let j = i + 1; j< str.length + 1; j++)
        {
            combinations.push(str.slice(i , j));
        }
    }
   return combinations;
}
console.log(possibleCombinations('Dog'));
// q4
function alphabet_order(str)
{
return str.split('').sort().join('');
}
console.log(alphabet_order("webmaster"));
// q5
function senenceCase(str) {
    var arr = str.split(""); 
    res = arr.sort().join(""); 
    return res; 
  }
  console.log("The Quick Brown fox");
// q6
function longestWord(string) {
    var str = string.split(" ");
    var longest = 0;
    var word = null;
    for (var i = 0; i < str.length - 1; i++) {
        if (longest < str[i].length) {
            longest = str[i].length;
            word = str[i];
        }
    }
    return word;
}

// q7
function vowel_count(str1)
{
  var vowel_list = 'aeiouAEIOU';
  var vcount = 0;
  
  for(var x = 0; x < str1.length ; x++)
  {
    if (vowel_list.indexOf(str1[x]) !== -1)
    {
      vcount += 1;
    }
  
  }
  return vcount;
}
console.log(vowel_count("The quick brown fox"));

// q8
function test_prime(n)
{

  if (n===1)
  {
    return false;
  }
  else if(n === 2)
  {
    return true;
  }else
  {
    for(var x = 2; x < n; x++)
    {
      if(n % x === 0)
      {
        return false;
      }
    }
    return true;  
  }
}

console.log(test_prime(37));
// q9
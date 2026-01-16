function check_palindrome(string){
let  bag=""
 for(let i=string.length-1;i>=0;i--){
  bag += string[i];
if(bag===string){
return true
}else{
return false
}
}
let ans= check_palindrome("ram");
if(ans==true){
console.log("Yes")
}else{
console.log("No")
}
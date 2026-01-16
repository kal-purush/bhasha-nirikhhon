

anagram_string = 'aa aa dog gdo odg';
console.log(countingAnagrams(anagram_string));

function removeSingleChar(word){
	return word.length > 1;
}
function sortWords(word){
	return word.split('').sort().join('');
}



function countingAnagrams(anagramString){
var count_hash = {};

removedAnagram = anagram_string.split(" ").filter(removeSingleChar);
sortedAnagram = removedAnagram.map(sortWords).sort();

for(var i=0; i < sortedAnagram.length; i++){
	if(Object.keys(count_hash).includes(sortedAnagram[i])){
  	count_hash[sortedAnagram[i]]++;
  }
  else{
  	count_hash[sortedAnagram[i]] = 0;
  }
}

return Object.values(count_hash).filter(function(val){
	return val > 0;
}).length;
}
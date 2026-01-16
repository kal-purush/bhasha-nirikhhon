// --- Directions
// Check to see if two provided strings are anagrams of eachother.
// One string is an anagram of another if it uses the same characters
// in the same quantity. Only consider characters, not spaces
// or punctuation.  Consider capital letters to be the same as lower case
// --- Examples
//   anagrams('rail safety', 'fairy tales') --> True
//   anagrams('RAIL! SAFETY!', 'fairy tales') --> True
//   anagrams('Hi there', 'Bye there') --> False

function anagrams(stringA, stringB) {
    let aCharMap = buildCharMap(stringA);
    let bCharMap = buildCharMap(stringB);
    if(Object.keys(aCharMap).length!==Object.keys(bCharMap).length)
        return false
    for(let char in aCharMap){
        if(aCharMap[char]!==bCharMap[char])
        return false
    }
    console.log(aCharMap,bCharMap);
    return true
}
function buildCharMap(str){
    const charMap = {};
    let stringVal = str.replace(/[^a-zA-Z0-9]/g,'').toLowerCase().split('');
    for(char of stringVal){
        charMap[char] = charMap[char]+1 || 1;
    }
   //console.log(charMap);
    return charMap;
}
//anagrams('aabcbd','AaDCBBB');
module.exports = anagrams;
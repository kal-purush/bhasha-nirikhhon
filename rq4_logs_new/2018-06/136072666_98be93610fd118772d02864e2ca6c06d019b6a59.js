function checkMagazine(magazine, note) {
    let hash = {};
    for (let i=0; i<magazine.length; i++) {
        if(hash[magazine[i]]=== undefined) {
            hash[magazine[i]] = 1
        } else {
            hash[magazine[i]] += 1
        }
    }
    for (let i=0; i<note.length; i++){
        if (hash[note[i]] <=0 ||hash[note[i]] === undefined) {
            return "No"
        } else {
            hash[note[i]] -=1
        }
    }
    return "Yes";

}

let magazine = ['give','me','one','grand','today','night']
let note = ['give','one','grand','today']
console.log(checkMagazine(magazine, note));
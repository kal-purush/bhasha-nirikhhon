function kthNonRepeating(string, k){

    if(k > string.length){
        return -1
    }

    let counts = Array(256).fill(string.length)

    for (let i = 0; i<string.length; i++) {
        let charCode = string.charCodeAt(i)
        
        if(counts[charCode] == string.length){
            // if not repeated 
            // the value at charCode will always be < string.length
            counts[charCode] = i
        }
        else{
            // if value already repeated then
            // it will make sure the value at charCode index will be > string.length
            // so later on we can sort the counts array and get kth element
            counts[charCode] = string.length + 2
        }
    }

    counts.sort((a,b)=>{return a-b})
    console.log(counts)

    return counts[k] < string.length ? string[counts[k]] : -1

}

console.log(kthNonRepeating('save soil is need', 3))
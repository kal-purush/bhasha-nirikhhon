function mastan (name1,name2='vai'){
    const fullName = `${name1} ${name2}`
    return fullName;
}
console.log(mastan('Bangla'))
console.log(mastan('yoyo','Gunda'))

function multi(x,y=3){
    return x*y;
}
console.log(multi(5))
console.log(multi(5))
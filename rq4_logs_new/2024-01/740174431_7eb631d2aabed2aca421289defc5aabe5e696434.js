const a = () => {
    return `I am A`;
}
/*
const b = () => {
    setTimeout(() => {
        return `Run after 3s`;
    }, 3000)
    return `I am B`;
}
*/

const b = () => {
    return new Promise((resolve, reject) => {
        setTimeout(() => {
            resolve(`I am B`);
        }, 3000);
    })
}
const c = () => {
    return `I am C`;
}

//i want c to execute when the execution of b is done
//--> we can do that by async
// a();
// c();
// b();

const callMe = async () => {
    const one = a();
    console.log(one);
    const two = await b();
    console.log(two);
    const three = c();
    console.log(three);
}
callMe();
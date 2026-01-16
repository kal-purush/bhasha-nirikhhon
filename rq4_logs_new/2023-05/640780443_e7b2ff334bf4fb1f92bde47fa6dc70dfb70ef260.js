//how to create promise- pending , resolve, reject
const promise1 = new Promise((resolve, reject) => {
    let completedPromise = true;
    if(completedPromise){
        resolve('Completed promise 1')
    }else{
        reject(new Error("not completed promise 1"))
    }
})

// console.log(promise1)

promise1.then((res)=>{
    console.log(res);
})
.catch((err) =>{
    console.log(err);
});
promise2.then((res) => console.log(res));
console.log("end");


//promise all
promise1.all([promise3, promise4])
.then(([res1,res2])=>console.log(res1,res2))
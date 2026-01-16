var deferredPrompt;

if ('serviceWorker' in navigator) {
    navigator.serviceWorker.register('/sw.js'
        // ,{
        //     scope: '/help'
        // }
    ).then(function () {
        console.log('ServiceWorker registered')
    }).catch(function (err) {
        console.log(err)
    })
};

// var promise = new Promise(function(resolve,reject){
//     setTimeout(function(){
//         resolve('Todo bien');
//         reject({code:500,mes:'Todo mal'})
//     }, 3000);
// })

// promise.then(function(text){
//     return text;
// }).then(function(newText){
//     console.log(newText)
// }).catch(function(err){
//     console.log(err.code, err.mes)
// })

fetch('https://httpbin.org/ip').then(function (res) {
    console.log(res);
    return res.json();
}).then(function (data) {
    console.log(data);
}).catch(function (err) {
    console.log(err)
})

fetch('https://httpbin.org/post', {
    method: 'POST',
    mode: 'cors',
    headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    },
    body: JSON.stringify({
        message: 'Does this work?'
    })
}).then(function (res) {
    console.log(res);
    return res.json();
}).then(function (data) {
    console.log(data);
}).catch(function (err) {
    console.log(err)
})



window.addEventListener('beforeinstallprompt', function (e) {
    e.preventDefault();
    deferredPrompt = e;
})
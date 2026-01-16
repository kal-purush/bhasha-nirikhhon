let stream$ = Rx.Observable.create(function (observer) {

    observer.next('One');

    setTimeout(function () {
        observer.next("After 3 seconds");
    }, 3000);

     setTimeout(function () {
        observer.error("Smth wrong");
    }, 5000);

    observer.next("Two");
});

stream$
    .subscribe(
        function (data) {
            console.log('Subscribe: ', data);
        },
        function(error){
            console.log("Error: " , error)
        },
        function(){
            console.log("Compleated!");
        }
    );
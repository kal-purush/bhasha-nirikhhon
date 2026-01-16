(function () {
    var source = Rx.Observable.create(function (observer) {
        // Yield a single value and complete
        observer.onNext(42);
        observer.onCompleted();

        // observer.onError('error');
      
        // Any cleanup logic might go here
        return function () {
          console.log('disposed');
        }
      });
      
      var subscription = source.subscribe(
        function (x) { console.log('onNext: %s', x); },
        function (e) { console.log('onError: %s', e); },
        function () { console.log('onCompleted'); });
      
      // => onNext: 42
      // => onCompleted
      
      subscription.dispose();
      // => disposed
})();
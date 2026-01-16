// let Observable = require('rxjs/Observable').Observable;;
var Rx = require('rx');
let superagent = require('superagent')
let apiUrl = `http://jsonplaceholder.typicode.com/users/1`;

apiUrl = 'http://jsonplaceholder.typicode.com/users/1';

exports.fetch_rx_data = function(req, res){
	let reactiveInspiration = Rx.Observable.create((observer) => {
    superagent
        .get(apiUrl)
        .end((err, res) => {
            if (err) {
                return observer.onError(err);
            }
            let data,
                username;
            data = res.body;
            username = data.username
            console.log(data.username)
            observer.onNext(username);
            observer.onCompleted();
        });
    return () => {
        console.log('destroy all');
    };
});

reactiveInspiration.subscribe({
    onNext: (username) => {
        console.log('username: ', username);
        res.json(username)
    },
    onError: (error) => {
        console.error('Error while getting username', error);
    },
    onCompleted: () => {
        console.log('completed');

    },
});
}
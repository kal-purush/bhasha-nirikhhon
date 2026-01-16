import WebWorker from "../../dist/web-worker";

class WebWorkerSample {

    static worker;

    static construct() {
        WebWorkerSample.worker = new WebWorker("js/worker.js");
        WebWorkerSample.worker.onmessage = function (e) {
            console.log(e);
            document.getElementById('result').textContent = e.data;
        };
    }

    static sayHI() {
        this.worker.postMessage({'cmd': 'start', 'msg': 'Hi'});
    }

    static stop() {
        // worker.terminate() from this script would also stop the worker.
        this.worker.postMessage({'cmd': 'stop', 'msg': 'Bye'});
    }

    static unknownCmd() {
        this.worker.postMessage({'cmd': 'foobard', 'msg': '???'});
    }
}

export default WebWorkerSample;
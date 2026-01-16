import * as stream from "../index";

////////////////////////////////////////////////////
/// Stream tests : http://nodejs.org/api/stream.html
////////////////////////////////////////////////////

// http://nodejs.org/api/stream.html#stream_readable_pipe_destination_options
function stream_readable_pipe_test() {
    var r = new stream.Readable();
    var z = new stream.Transform();
    var w = new stream.Writable();
    r.pipe(z).pipe(w);
}
try {
    countLineNumber();
} catch (e) {
    console.log(e);
}

function countLineNumber() {
    var i;
    var count = 0;
    require('fs').createReadStream('file.txt')
        .on('data', function (chunk) {
            for (i = 0; i < chunk.length; ++i)
                if (chunk[i] == 10) count++;
        })
        .on('end', function () {
            console.log("Total Number Lines " + count);
        });
}
/* Second Try, using `data` and `end` events
 * The code has not been tested, here to give the general idea
 * Need to write these things down, very important */

const MAX_BATCH = 50; // how many promises/API calls are queued at once

let JSONStream = {} // I live in the outer closure, so I'm available to all functions in this context

let promiseArray = []; // I hold the promises within a batch
let batchCount = 0;

JSONStream.on(`data`, (chunk) => {

    // create a promise for each chunk, and add it to promiseArray
    batchCount++;
    let p = new Promise((resolve, reject) => {
        try {
            let geoCode = '';
            // Add geocode stuff here
            // ...
            // End geocode stuff

            resolve(geoCode);
            
            // set all variables to null after the promise resolves to encourage garbage collection (explicitly break references)
            geocode = null;
        }
        catch (err) {
            reject(err);
        }
    });

    promiseArray.push(p
        .then(data => ({ geoCode: data })) // Add all geocodes to an object - will be aggregated by Promise.all
        .catch(err => ({ errors: err })) // Add all error to an object - will be aggregated by Promise.all
        // This also guarantees that the promise chain will always resolve successfully, prevent early Promise.all resolution
		// shout-out to pipakin for doing this in courtbot-engine
    );

    // null out to encourage garbage collection for large data sets
    p = null;

    if (batchCount === MAX_BATCH) {
        // Prevent further event triggering
        JSONStream.pause();

        // Use Promise.all to wait for everything to finish processing, for better or worse
        // No catch statement, since all errors are processed earlier
        Promise.all(promiseArray)
            .then((results) => {
                let batch = results.reduce((aggregate, item) => {
					item.error ? aggregate.errors.push(item.error) : aggregate.geoCodes.push(item.geoCode);
				}, { geoCodes: [], errors: [] });

                // batch is now an object with an array of geoCodes and an array of errors
                processBatch(batch);

                // null out to encourage garbage collection for large data sets
                results = batch = promiseArray = null;

                // reset batch vars
                batchCount = 0;
                promiseArray = [];

                // resume the stream
                JSONStream.resume();
            });
    }
});

// Force processing of a batch when the stream ends
JSONStream.on(`end`, () => {
    Promise.all(promiseArray)
        .then((results) => {
            let batch = results.reduce((aggregate, item) => {
				item.error ? aggregate.errors.push(item.error) : aggregate.geoCodes.push(item.geoCode);
			}, { geoCodes: [], errors: [] });

            // batch is now an object with an array of geoCodes and an array of errors
            processBatch(batch);

            // null out to encourage garbage collection for large data sets
            results = batch = promiseArray = null;
    });
});

// I process a batch of geoCodes
function processBatch(batch) {
    // fill out function stub;
}
const rp = require('request-promise');
const fs = require('fs');
const lineByLine = require('n-readlines');
const args = process.argv;
let MongoClient = require('mongodb').MongoClient;
let uri = "mongodb://localhost:27017/";

if (args.length !== 5) {
	console.log('Usage: node getCrossrefWorks.js <input_file> <data_output_file> <not_found_dois_output_file>');
	process.exit(-1);
}

const liner = new lineByLine(args[2]);
const dataOutputFile = args[3];
const doisOutputFile = args[4];

function connect() {
	return new Promise( (resolve, reject) => {
		MongoClient.connect(uri, (err, conn) => {
			if (err) {
				reject(err);
			} else {
				let db = conn.db('pub_finder');
				resolve(db);
			}
		});
	});
}

function query(conn, col, doi) {
	let query = {
    	doi: doi,
    };

    return new Promise( (resolve, reject) => {
	    conn.collection(col).find(query).toArray( (err, result) => {
			if (err) {
				reject(err);
			} else {
				resolve(result);
			}
		});
	});
}

function removeNewLine(str) {
	// removes new line & multiple spaces
	return str.replace(/[\n\r]+/g, ' ').replace(/\s{2,}/g,' ').trim();
}

function transform(result, source) {
	return `"${result.doi}"\t"${removeNewLine(result.title || '')}"\t"${(result.authors || []).map( (a) => removeNewLine(a.name)).join(', ')}"\t"${removeNewLine(result.venue || '')}"\t"${removeNewLine(result.abstract || '')}"\t"${result.year || ''}"\t"${source}"\n`;
}
async function getPapers() {
	let dataStream = fs.createWriteStream(dataOutputFile, {flags:'a'});
	let doisStream = fs.createWriteStream(doisOutputFile, {flags:'a'});

    let conn = await connect();
    let count = 0;

	let line;
	while (line = liner.next()) {
	    let doi = line.toString('ascii');
	    try {
		    let result = await query(conn, 'aminer', doi);
		    if (result.length > 0) {
		    	dataStream.write(transform(result[0], 'aminer'));
		    } else {
		    	let result = await query(conn, 'mag', doi);
		    	if (result.length > 0) {
		    		dataStream.write(transform(result[0], 'mag'));
		    	} else {
		    		doisStream.write(doi + "\n");
		    	}
		    }
		    count++;
		    console.log(count);
		} catch (err) {
			console.log(err);
		}
	}

	conn.close();
	dataStream.end();
	doisStream.end();
}

getPapers();
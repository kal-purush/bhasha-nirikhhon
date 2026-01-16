const {getDb, getNextSequence} = require('./db.js');

//when is false; how to get poster_id
async function add(_, { posting }) {
    const db = getDb();

    const newPosting = Object.assign({}, posting);
    newPosting.created_at = new Date();
    newPosting.id = await getNextSequence("postings");

    const result = await db.collection('postings').insertOne(newPosting);
    return true;
}

async function delet(_, { posting_id }) {
    const db = getDb();
    const deletedPosting = await db.collection("postings").findOne({posting_id:posting_id});
    if (deletedPosting == null) {
        return false
    }
    else {
        const result = await db.collection('postings').deleteOne({posting_id:posting_id});
        return true;
    }
}

async function Inf() {
    const db = getDb();
    const postings = await db.collection("postings").find({}).toArray();
	return postings
}

module.export = { add, delet, Inf};
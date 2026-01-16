// user: uttam
// pass:Uttam1234
const mongoose = require('mongoose');

// const url = 'mongodb+srv://uttam:Uttam1234@cluster0.l3wuu.mongodb.net/?retryWrites=true&w=majority';

// module.exports.connect = () => {
// 	mongoose
// 		.connect(url, {
// 			useNewUrlParser: true,
// 			useUnifiedTopology: true,
// 		})
// 		.then(() => {
// 			console.log('MongoDB Connected Successfully');
// 		})
// 		.catch((error) => console.log('Error:', error));
// };

// const database = (module.exports = () => {
// 	const connectionParams = {
// 		useNewUrlParser: true,
// 		useUnifiedTopology: true,
// 	};
// 	try {
// 		mongoose.connect(url);
// 		console.log('MongoDB is connected');
// 	} catch (error) {
// 		console.log(error);
// 		console.log('connection failed!! please check your connection');
// 	}
// });

// database();
// module.exports = database;

mongoose.connect('mongodb://localhost/quora-clone');

const db = mongoose.connection;

db.on('error', console.error.bind(console, 'error connecting to Mongo DB'));

db.once('open', function () {
	console.log('connected to database :: MongoDB');
});

module.exports = db;
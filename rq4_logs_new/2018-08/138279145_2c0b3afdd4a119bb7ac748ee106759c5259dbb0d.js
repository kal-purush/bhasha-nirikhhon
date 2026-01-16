import mongoose from 'mongoose';
import colors from 'colors';
mongoose.Promise = Promise;

const connectDB = function(url){
	mongoose.connect(process.env.DB_HOST + url);
	const db = mongoose.connection;

	db.on('error', err => {
		if (process.env.MODE === 'development'){
			console.log(colors.red('DataBase connection error:', err.message));
		}
		
	});

	db.once('connected', () => {
		console.log('DataBase successfully connected to:'.green, process.env.DB_URL);
	});		
};

export { connectDB }
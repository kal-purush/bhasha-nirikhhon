//  Multer has file filters! Check it out!
import multer from 'multer';
import path from 'path';
import crypto from 'crypto';

export default {
	storage: multer.diskStorage({
		destination: path.resolve(__dirname, '..', '..', 'uploads'),
		filename: (req, file, callback) => {
			// Hash (hex-decimal string) with 6 random chars bytes
			const hash = crypto.randomBytes(6).toString('hex');

			// Parsing name
			const parsedName = `${hash}-${file.originalname}`;

			callback(null, parsedName);
		}
	})
};
const Crypto = require('crypto-js');
const fs = require('fs');
const path = require('path');

class Fcrypt {
	static #writeFile({ fileName, newFileName, data }) {
		fs.writeFileSync(newFileName, data);
		fs.rmSync(fileName);
	}

	static #readFile(filPath) {
		const text = fs.readFileSync(filPath, 'utf8');
		if (!text.length) throw new Error('File is empty');
		return text;
	}

	static encrypt(file, secretKey) {
		const filePath = path.join(__dirname, '..', file);
		const plainText = this.#readFile(filePath);

		if (secretKey.length < 16) throw new Error('Secret key is too short');

		const cipherText = Crypto.AES.encrypt(plainText, secretKey).toString();

		this.#writeFile({
			fileName: filePath,
			newFileName: filePath + '.enc',
			data: cipherText,
		});
	}

	static decrypt(file, secretKey) {
		if (!file.match(/\.enc$/)) return;

		const filePath = path.join(__dirname, '..', file);
		const encryptedText = this.#readFile(filePath);
		const plainText = Crypto.AES.decrypt(encryptedText, secretKey).toString(
			Crypto.enc.Utf8
		);

		if (!plainText.length)
			throw new Error('Failed to decrypt, wrong secret key!');

		this.#writeFile({
			fileName: filePath,
			newFileName: filePath.slice(0, -4),
			data: plainText,
		});
	}
}

module.exports = Fcrypt;
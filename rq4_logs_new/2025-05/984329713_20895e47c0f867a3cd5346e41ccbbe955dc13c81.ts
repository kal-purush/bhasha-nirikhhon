import crypto from "node:crypto";
import { readFileSync, rmSync, writeFileSync } from "node:fs";
import readlineSync from "readline-sync";
import { Readable } from "stream";
import { create, extract } from "tar";

export const algorithm = "aes-256-cbc";

export function getPassword() {
	const key = process.env.DATABASE_PASSWORD ?? readlineSync.question("Password: ");
	return crypto.createHash("sha256").update(key).digest("base64").substring(0, 32);
}

export function decrypt(inPath: string) {
	const key = getPassword();
	const file = readFileSync(inPath);
	const iv = file.subarray(0, 16);
	const encrypted = file.subarray(16);
	const decipher = crypto.createDecipheriv(algorithm, key, iv);
	const result = Buffer.concat([decipher.update(encrypted), decipher.final()]);
	Readable.from(result).pipe(extract({
		gzip: true,
		cwd: "src/db",
		sync: true,
		onReadEntry(entry) {
			entry.path = "data.db";
		},
	}));
}

export async function encrypt(outPath: string) {
	const file = "db.tgz";
	create({
		file,
		gzip: true,
		sync: true,
		cwd: "src/db",
	}, ["data.db"]);
	const buffer = readFileSync(file);
	const key = getPassword();
	// Create an initialization vector
	const iv = crypto.randomBytes(16);
	const cipher = crypto.createCipheriv(algorithm, key, iv);
	// Create the new (encrypted) buffer
	writeFileSync(outPath, Buffer.concat([iv, cipher.update(buffer), cipher.final()]));
	rmSync(file);
}
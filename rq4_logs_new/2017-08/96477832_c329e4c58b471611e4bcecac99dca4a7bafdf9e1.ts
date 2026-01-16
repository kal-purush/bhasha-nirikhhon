import { S3 } from "aws-sdk";
import * as fs from "fs";

import { AuthStorage } from "./AuthStorage";

export class AwsS3 implements AuthStorage {
	private static readonly BUCKET_NAME = "dream-swing-automation-scripts";
	private static readonly SYNC_TOKEN_NAME = "google-calendar-sync-token";

	private static readonly ZIP_FILENAME = "google-calendar-to-trello.zip";
	private static readonly ZIP_LOCAL_PATH = "out/" + AwsS3.ZIP_FILENAME;

	private _s3: S3;

	constructor() {
		// credentials stored locally in ~/.aws/credentials
		this._s3 = new S3();
	}

	public uploadZip() {
		let zipReadStream = fs.createReadStream(AwsS3.ZIP_LOCAL_PATH);
		let uploadParams = {
			Bucket: AwsS3.BUCKET_NAME,
			Key: AwsS3.ZIP_FILENAME,
			Body: zipReadStream
		};
		this._s3.upload(uploadParams, (err, data) => {
			if (err) {
				throw new Error(`Storing '${AwsS3.ZIP_FILENAME}' to S3 failed. ${err}`);
			}

			console.log(`Storing '${AwsS3.ZIP_FILENAME}' to S3 succeeded. ETag: ${data.ETag}. Location: ${data.Location}`);
		});
	}

	public storeSyncToken(syncToken: string) {
		this.storeData(AwsS3.SYNC_TOKEN_NAME, syncToken, "text/plain", /*encrypt*/false);
	}

	public getSyncToken(callback) {
		this.getData(AwsS3.SYNC_TOKEN_NAME, (dataBody) => {
			let token = dataBody.toString();
			console.log("token from storage: " + token);
			callback(token);
		});
	}

	public storeEncryptedAuth(key: string, authData: any) {
		this.storeData(key, authData, "application/json", /*encrypt*/true);
	}

	public getEncryptedAuth(key: string, callback: ((string) => void)) {
		this.getData(key, (dataBody) => {
			let jsonAuth = JSON.parse(dataBody.toString());
			callback(jsonAuth);
		});
	}

	private storeData(key: string, bodyContent, contentType: string, encrypt: boolean) {
		this.ensureBucketExist();

		this._s3.waitFor("bucketExists", { Bucket: AwsS3.BUCKET_NAME }, (err, data) => {
			if (typeof bodyContent !== "string") {
				bodyContent = JSON.stringify(bodyContent);
			}

			let putParams = {
				Bucket: AwsS3.BUCKET_NAME,
				Key: key,
				Body: bodyContent,
				ContentType: contentType
			};
			if (encrypt) {
				putParams["ServerSideEncryption"] = "AES256";
			}
			this._s3.putObject(putParams, (err, data) => {
				if (err) {
					throw new Error(`Storing data to key ${key} failed. ${err}`);
				}

				console.log(`${key} data stored successfully. ETag: ${data.ETag}`);
			});
		});
	}

	private getData(key: string, callback) {
		this.bucketExist((doesExist) => {
			if (!doesExist) {
				throw new Error("Bucket does not exist, can't retrieve data.");
			}

			let getParams = {
				Bucket: AwsS3.BUCKET_NAME,
				Key: key
			};
			this._s3.getObject(getParams, (err, data) => {
				if (err) {
					throw new Error(`Could not retrieve data for ${key}. ${err}`);
				}
				callback(data.Body);
			});
		});
	}

	private ensureBucketExist() {
		this.bucketExist((doesExist) => {
			if (!doesExist) {
				let createParams = {
					Bucket: AwsS3.BUCKET_NAME,
					CreateBucketConfiguration: {
						LocationConstraint: "us-east-1"
					}
				}
				this._s3.createBucket(createParams, (err, data) => {
					if (err) {
						throw new Error("Error creating S3 bucket. " + err);
					}

					console.log(`Bucket ${AwsS3.BUCKET_NAME} created in location: ${data.Location}`);
				});
			}
		});
	}

	private bucketExist(callback) {
		this._s3.headBucket({ Bucket: AwsS3.BUCKET_NAME }, (err, data) => {
			if (err) {
				console.log("headBucket returned error. " + err);
				callback(false);
			} else {
				callback(true);
			}
		})
	}
}











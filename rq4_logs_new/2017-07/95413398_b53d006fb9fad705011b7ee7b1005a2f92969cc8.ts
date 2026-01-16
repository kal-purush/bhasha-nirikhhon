import { Injectable } from '@angular/core';

import * as firebase from 'firebase';

@Injectable()
export class StorageService {

	public uploadImage(uid: string, file: File, username: string): Promise<string> {
		console.log(file);
		return new Promise((resolve, reject) => {
			firebase.storage().ref('/Users/' + uid + '/' + username + ' Profile Picture')
				.put(file).then(snapshot => {
					console.log('Image uploaded.')
					resolve(snapshot.downloadURL);
				});
		});
	}

	public uploadResource(uid: string, file: File): Promise<string> {
		return new Promise((resolve, reject) => {
			firebase.storage().ref('/Users/' + uid + '/Resources/' + file.name)
				.put(file).then((snapshot) => {
					console.log('File uploaded.')
					resolve(snapshot.downloadURL);
				});
		});
	}

}
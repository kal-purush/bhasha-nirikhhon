import firebase from 'firebase';

class FirebaseCDB {

    static getCloudStorage() {
        firebase.initializeApp({
            apiKey: 'AIzaSyB9Rs70Wah95RipfpEdDUnvIrDRVyoDzi8',
            authDomain: 'fireb-ui.firebaseapp.com',
            projectId: 'fireb-ui'
        });
        const firestore = firebase.firestore();
        firestore.settings({ timestampsInSnapshots: true });
        return firebase.firestore();
    }

    static getUserReference(){
        return this.getCloudStorage().collection('users').doc('Gc6WWHLaCyolovaLO2A8');
    }
}

export default FirebaseCDB;
class ActivityService {

    constructor(props) {
        super(props);
    }

    getAll() {
        var firebaseAllActivityRef = firebase.database.ref('activities');
        firebaseAllActivityRef.on('value', function (acitvitysnapshot) {
            return acitvitysnapshot.val();
        })

    }

    getById(id) {
        var firebaseIdActivityRef = firebase.database.ref('activities').child('activities/user_id');
        firebaseIdActivityRef.on('value', function (idactivitysnapshot) {
            return idactivitysnapshot.val();
        })
    }

    // getByEmail(email) {
    // }
}

export default ActivityService;
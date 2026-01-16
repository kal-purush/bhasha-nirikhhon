
// Your web app's Firebase configuration
var firebaseConfig = {
    apiKey: "AIzaSyDaO947iRR27ATA2CnosYY4xYwNLtHDVU8",
    authDomain: "skyline-9c630.firebaseapp.com",
    databaseURL: "https://skyline-9c630.firebaseio.com",
    projectId: "skyline-9c630",
    storageBucket: "",
    messagingSenderId: "231428382310",
    appId: "1:231428382310:web:988a6dcbac349fb2"
};
// Initialize Firebase
firebase.initializeApp(firebaseConfig);

var db = firebase.firestore();
const increment = firebase.firestore.FieldValue.increment(1);
// random string length

var randomString = function (length) {

    var text = "";

    var possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    for (var i = 0; i < length; i++) {

        text += possible.charAt(Math.floor(Math.random() * possible.length));

    }

    return text;
}

function query(e) {
    e.preventDefault();
    var find = Number(document.getElementById('qpos').value);
    //find = Number(find);    
    db.collection("Individual Position").where("Position","==",find).get().
        then(function (querySnapshot) {
            querySnapshot.forEach(function(doc) {
                
                console.log(doc.data());
                
                console.log(doc.data().UID);
                if (doc.data().UID==`${document.getElementById('uid').value}`)
                {
                    window.alert('You are successfully verifed');
                    doc.ref.delete();
                   /*  
                 
                    db.collection("Individual Position").doc('2jlIKpy4HOP5EFf6yH5R').delete().then(function () {
                        console.log("Document successfully deleted!");
                    }).catch(function (error) {
                        console.error("Error removing document: ", error);
                    }); */
                }
                else{
                    window.alert('Verification Failed');
                    document.location.reload(true);
                }
            });
        }).catch(function (error) {
            console.log("Error getting documents: ", error);
        }
        )
}
var random = randomString(10);
function register(e) {
    e.preventDefault();
    db.collection("Queue").doc('Queue Number').get().then(function (doc) {
   
})
db.collection('Queue').doc('Queue Number').update({position: increment})
.then(function () {
    DisplayQ();
}).catch(function(error){
    console.error("Error Queing : ",error);
});
}



function DisplayQ(){

    db.collection("Queue").doc('Queue Number').get().then(function (doc) {

        var d = new Date();
        var n = d.toTimeString();
        console.log(`${doc.id} => ${doc.data().position}`);
        db.collection("Individual Position").add({
            Name: document.getElementById('name').value,
            Email: document.getElementById('email').value,
            Phone_no: document.getElementById('phno').value,
            Address: document.getElementById('address').value,
            Position: doc.data().position,
            UID: random,
            Time: n
        }).then(function(){
            var str;
            str += `<tr>
            <th>Order ID</th>
            <th>Phone Number</th>
            <th>Name</th>
            <th>Address</th>
            <th>UID</th>
        </tr>
        <tr>
            <td>${doc.data().position}</td>
            <td>${document.getElementById('phno').value}</td>
            <td>${document.getElementById('name').value }</td>
            <td>${document.getElementById('address').value }</td>
            <td>${random}</td>
            </tr>`;
        document.getElementById('show').innerHTML = str;
        }).catch(function (error) {
            console.error(error);
        })
        var email = document.getElementById("email").value;
        var xhr = new XMLHttpRequest();
        xhr.open('GET', "https://us-central1-pitney-a57f5.cloudfunctions.net/sendMail?dest=" + email + "&uid=" + random + "&oid=" + doc.data().position, true);
        xhr.send();
        if (xhr.status = 200) {
            window.alert("Mail has been sent!");
        }
    });
}

function live() {
    db.collection("Individual Position").orderBy('Time').get().then((querySnapshot) => {
        querySnapshot.forEach((doc) => {
           var str = `<tr>
            <td>${doc.data().Position}</td>
            <td>${doc.data().Phone_no}</td>
            <td>${doc.data().Address}</td>
           
            </tr>`;
            document.getElementById("live_details").innerHTML += str;
        });
    });
}

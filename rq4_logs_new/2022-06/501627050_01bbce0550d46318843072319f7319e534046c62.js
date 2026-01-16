const express = require('express');
const app = express();
const port = 8080;

const admin = require("firebase-admin");
const { v4: uuidv4 } = require('uuid');
const serviceAccount = require("./fcmmsg.json");
const registrationToken = 'eMLKfUFmoYw:APA91bE_H14CfoX6Ty5h0_61M9GPd0G5Z_rTaqGoy7qwBAPybJyrHnuxqcMh0YKuyTxB3cY-W-7xELqERACkEcXExxuENcpakvllDsy2yrEWZ6AdmTPC5fzv1FfOx2mkJYifRc_kEasF';

admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    storageBucket: "fcmmsg-cfe6a.appspot.com/images"
});

const storageRef = admin.storage().bucket('gs://fcmmsg-cfe6a.appspot.com');

// firebase storage에 업로드하는 함수
async function uploadFile(path, filename) {
    // Upload the File
    var storage = await storageRef.upload(path, {
        public: true,
        destination: `${filename}`,
        metadata: {
        firebaseStorageDownloadTokens: uuidv4(),
        }
    });
    // console.log(storage[0].metadata.mediaLink);
    return storage[0].metadata.mediaLink;
}

// 푸시 알림으로 전송할 내용
// var imgname = 'screenshot.jpg';
// var imgurl = 'https://firebasestorage.googleapis.com/v0/b/fcmmsg-cfe6a.appspot.com/o/' + imgname + '?alt=media';
// var message = {
//     notification: {
//     title: '긴급 알림',
//     body: '위치 알림 메시지입니다.',
//     imageUrl: imgurl
//     },
//     token: registrationToken
// };

app.get('/push_alarm', () => {
    // 업로드 실행
    (async () => {
      const url = await uploadFile('./images/screenshot.jpg', "screenshot.jpg");
      console.log(url);
    })();

    var imgname = 'screenshot.jpg';
    var imgurl = 'https://firebasestorage.googleapis.com/v0/b/fcmmsg-cfe6a.appspot.com/o/' + imgname + '?alt=media';
    var message = {
        notification: {
            title: '긴급 알림',
            body: '위치 알림 메시지입니다.',
            imageUrl: imgurl
        },
    token: registrationToken
    };

    // 푸시 알림 보내기
    admin.messaging().send(message)
      .then((response) => {
        console.log('Successfully sent message:', response);
      })
      .catch((error) => {
        console.log('Error sending message:', error);
      });
});

app.use(express.static(__dirname + '/public'));

app.listen(port, function(){
    console.log('listening on *:' + port);
});
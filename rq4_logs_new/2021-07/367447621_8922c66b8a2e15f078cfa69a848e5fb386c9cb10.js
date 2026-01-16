const fs = require("fs")
const readline = require("readline")
const { google } = require("googleapis")
var path = require('path');




const KEYFILEPATH = path.join(__dirname, 'service_account.json')
const SCOPES = ['https://www.googleapis.com/auth/drive'];

const auth = new google.auth.GoogleAuth(
    opts = {
        keyFile: KEYFILEPATH,
        scopes: SCOPES
    }
);
const driveService = google.drive(options = { version: 'v3', auth });

const filePath = path.join(__dirname, 'slide-1.jpeg')

async function createAndUploadFile(auth) {

    let fileMetaData = {
        "name": "slide-2.jpg",
        "parents": ["1evgjhxMA8DujwwkvMpaXIAqA_GigLJes"]

    }
    let media = {
        mimeType: 'image/jpeg',
        body: fs.createReadStream(path = "slide-2.jpg")
    }
    let responese = await driveService.files.create(param = {
        resource: fileMetaData,
        media: media,
        // fields: 'id'
    })
    await driveService.permissions.create({
        fileId: responese.data.id,
        requestBody: {
            role: 'reader',
            type: 'anyone',
        },
    });
    switch (responese.status) {
        case 200:
            console.log("file create id", responese.data.id)
            break;
    }
}
//createAndUploadFile(auth).catch(console.error);




async function deleteFile(auth) {
    let responese = await driveService.files.delete(param = {
        fileId: "1-tlHcJKVPiUyX2h6Wy-qVGQGg4By0LMn",
    })
    switch (responese.status) {
        case 200:
            console.log("file create id", responese.data.id)
            break;
    }
}
// deleteFile(auth).catch(console.error);

async function createFile(auth) {
    var fileMetadata = {
        'name': 'Invoices',
        'mimeType': 'application/vnd.google-apps.folder',
        parents: ["1POrUCCGc43XGxnIFjoezycE3YZGPStUJ"]
    };
    let responese = await driveService.files.create({
        resource: fileMetadata,
        fields: 'id'
    })
    switch (responese.status) {
        case 200:
            console.log("file create id", responese.data.id)
            break;
    }
}
// createFile(auth).catch(console.error);
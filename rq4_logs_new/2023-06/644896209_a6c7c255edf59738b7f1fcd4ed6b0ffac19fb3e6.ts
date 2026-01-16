import hmacSHA256 from 'crypto-js/hmac-sha256';
import Base64 from 'crypto-js/enc-base64';

const ApiKey = 'Config.API_KEY';
const Secret = 'Config.SECRET';

const getToken = async () => {
  const tokenData = 'await AuthData.state';
  const jwt = tokenData;
  return jwt;
};

const baseURL = 'Config.API_URL';
let myHeaders = new Headers();
myHeaders.append('Content-Type', 'application/json');
const requestOptions = (headers: Headers, raw: string) => ({
  method: 'POST',
  headers: headers,
  body: raw,
  redirect: 'follow',
});

const generateRandomString = (lenth: number) => {
  const char = 'AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz1234567890';
  const random = Array.from(
    {length: lenth},
    () => char[Math.floor(Math.random() * char.length)],
  );
  return random.join('');
};

export const loginRequest = async (userName: string, password: any) => {
  var Nonce = generateRandomString(32);
  var s = userName + ':' + 'lab.tagroot.io' + ':' + Nonce;
  const hmacDigest = Base64.stringify(hmacSHA256(s, password));

  let raw = JSON.stringify({
    userName: userName,
    nonce: Nonce,
    signature: hmacDigest,
    seconds: 3500,
  });
  let headers = new Headers();
  headers.append('Content-Type', 'application/json');
  const Response = await fetch(
    baseURL + 'Account/Login',
    requestOptions(headers, raw),
  )
    .then(response => response.json())
    .then(response => {
      return response;
    })
    .catch(error => console.log('error', error));
  return Response;
};

export const createAccountRequest = async (userName: string, email: string, password: string) => {
  var Nonce = generateRandomString(32);
  const host = 'lab.tagroot.io';
  var s =
    userName +
    ':' +
    host +
    ':' +
    email +
    ':' +
    password +
    ':' +
    ApiKey +
    ':' +
    Nonce;
  const signature = Base64.stringify(hmacSHA256(s, Secret));
  const raw = JSON.stringify({
    userName: userName,
    eMail: email,
    password: password,
    apiKey: ApiKey,
    nonce: Nonce,
    signature: signature,
    seconds: 3500,
  });
  let headers = new Headers();
  headers.append('Content-Type', 'application/json');
  const Response = await fetch(
    baseURL + 'Account/Create',
    requestOptions(headers, raw),
  )
    .then(response => response.json())
    .then(response => {
      return response;
    })
    .catch(error => console.log('error', error));
  return Response;
};

export const verifyEmailRequest = async (email: any, code: any) => {
  const token = await getToken();
  let headers = new Headers();
  headers.append('Content-Type', 'application/json');
  headers.append('Authorization', `Bearer ${token}`);
  const raw = JSON.stringify({
    eMail: email,
    code: code,
  });
  const Response = await fetch(
    baseURL + 'Account/VerifyEMail',
    requestOptions(headers, raw),
  )
    .then(response => response.text())
    .then(response => {
      return response;
    })
    .catch(error => console.log('error', error));
  return Response;
};

export const generatePublicKeyRequest = async () => {
  const token = await getToken();
  let headers = new Headers();
  headers.append('Content-Type', 'application/json');
  headers.append('Authorization', `Bearer ${token}`);
  var raw = JSON.stringify({});
  const Response = await fetch(
    baseURL + 'Crypto/GetPublicKey',
    requestOptions(headers, raw),
  )
    .then(response => response.json())
    .then(response => {
      return response;
    })
    .catch(error => console.log('error', error));
  return Response;
};

export const verifyKeyIDRequest = async (
  userName: string,
  localName: string,
  namespace: string,
  keyID: string,
  keyPassword: any,
  accountPassword: any,
) => {
  const token = await getToken();
  let headers = new Headers();
  headers.append('Content-Type', 'application/json');
  headers.append('Authorization', `Bearer ${token}`);
  const Nonce = generateRandomString(32);
  const s1 =
    userName +
    ':' +
    'lab.tagroot.io' +
    ':' +
    localName +
    ':' +
    namespace +
    ':' +
    keyID;
  const KeySignature = Base64.stringify(hmacSHA256(s1, keyPassword));
  const s2 = s1 + ':' + KeySignature + ':' + Nonce;
  const RequestSignature = Base64.stringify(hmacSHA256(s2, accountPassword));

  const raw = JSON.stringify({
    localName: localName,
    namespace: namespace,
    id: keyID,
    nonce: Nonce,
    keySignature: KeySignature,
    requestSignature: RequestSignature,
  });
  const Response = await fetch(
    baseURL + 'Crypto/CreateKey',
    requestOptions(headers, raw),
  )
    .then(response => response.json())
    .then(response => {
      console.log(response);
      return response;
    })
    .catch(error => console.log('error', error));
  return Response;
};

export const createLegalIdRequest = async (
  userName: string,
  localName: string,
  namespace: string,
  keyID: string,
  keyPassword: any,
  accountPassword: any,
  Properties: { [x: string]: any; },
) => {
  try {
    const token = await getToken();
    let headers = new Headers();
    headers.append('Content-Type', 'application/json');
    headers.append('Authorization', `Bearer ${token}`);
    headers.append('Referer', 'lab.tagroot.io');
    var Nonce = generateRandomString(32);
    var s1 =
      userName +
      ':' +
      'lab.tagroot.io' +
      ':' +
      localName +
      ':' +
      namespace +
      ':' +
      keyID;

    var KeySignature = Base64.stringify(hmacSHA256(s1, keyPassword));
    var s2 = s1 + ':' + KeySignature + ':' + Nonce;
    var PropertiesVector = [];

    for (var PropertyName in Properties) {
      var PropertyValue = Properties[PropertyName];
      s2 += ':' + PropertyValue.name + ':' + PropertyValue.value;
      PropertiesVector.push({
        name: PropertyValue.name,
        value: PropertyValue.value,
      });
    }
    const RequestSignature = Base64.stringify(hmacSHA256(s2, accountPassword));
    const raw = JSON.stringify({
      keyId: keyID,
      nonce: Nonce,
      keySignature: KeySignature,
      requestSignature: RequestSignature,
      Properties: Properties,
    });
    const Response = await fetch(
      baseURL + 'Legal/ApplyId',
      requestOptions(headers, raw),
    )
      .then(response => {
        console.log(response);
        return response;
      })
      .catch(error => console.log('error', error));
    return Response;
  } catch (error) {
    console.log(error);
  }
};

export const addAttachmentToLegalIdRequest = async (
  userName: string,
  localName: string,
  namespace: string,
  keyID: string,
  legalID: any,
  keyPassword: any,
  accountPassword: any,
  Attachment: { base64: string; fileName: string; type: string; },
) => {
  const token = await getToken();
  let headers = new Headers();
  headers.append('Content-Type', 'application/json');
  headers.append('Authorization', `Bearer ${token}`);
  const Nonce = generateRandomString(32);
  console.log('nonce = ', Nonce);
  const s1 =
    userName +
    ':' +
    'lab.tagroot.io' +
    ':' +
    localName +
    ':' +
    namespace +
    ':' +
    keyID;

  const KeySignature = Base64.stringify(hmacSHA256(s1, keyPassword));
  const s2 =
    s1 +
    ':' +
    KeySignature +
    ':' +
    Nonce +
    ':' +
    Attachment.base64 +
    ':' +
    Attachment.fileName +
    ':' +
    Attachment.type;

  const RequestSignature = Base64.stringify(hmacSHA256(s2, accountPassword));
  const raw = JSON.stringify({
    keyId: keyID,
    legalId: legalID,
    nonce: Nonce,
    keySignature: KeySignature,
    requestSignature: RequestSignature,
    attachmentBase64: Attachment.base64,
    attachmentFileName: Attachment.fileName,
    attachmentContentType: Attachment.type,
  });

  const Response = await fetch(
    baseURL + 'Legal/AddIdAttachment',
    requestOptions(headers, raw),
  )
    .then(response => {
      return response;
    })
    .catch(error => console.log('error', error));
  return Response;
};

export const legalIdApprovalRequest = async (legalID: any) => {
  const token = await getToken();
  let headers = new Headers();
  headers.append('Content-Type', 'application/json');
  headers.append('Authorization', `Bearer ${token}`);
  const raw = JSON.stringify({
    legalId: legalID,
  });
  const Response = await fetch(
    baseURL + 'Legal/ReadyForApproval',
    requestOptions(headers, raw),
  )
    .then(response => {
      return response;
    })
    .catch(error => console.log('error', error));
  return Response;
};
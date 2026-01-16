import {
  storeUserSession,
  retrieveUserSession,
  removeUserSession,
} from '../Storage';
import { TextEncoder } from 'text-decoding';
import hmacSHA256 from 'crypto-js/hmac-sha256';
import Base64 from 'crypto-js/enc-base64';
import Config from 'react-native-config';

const host = Config.Host;
const ApiKey = Config.ApiKey;
const Secret = Config.Secret;
const Seconds = 3500;

export const OnboardingAPI = {
  IO: {
    Request: async function (
      Resource: string,
      RequestPayload: any,
      Internal?: any,
    ) {
      const Request = new Promise(async (SetResult, SetException) => {
        let xhttp = new XMLHttpRequest();
        xhttp.onreadystatechange = function () {
          if (xhttp.readyState == 4) {
            let Response = xhttp.responseText;
            if (xhttp.status === 200) {
              Response = JSON.parse(Response);
              SetResult(Response);
            } else SetException(Response);

            if (!Internal) OnboardingAPI.IO.AfterResponse(Response);
          }
        };

        if (!Internal) this.BeforeRequest(RequestPayload);

        xhttp.open('POST', Config.ID_API_URL + Resource);
        xhttp.setRequestHeader('Content-Type', 'application/json');
        xhttp.setRequestHeader('Accept', 'application/json');

        var Token = await OnboardingAPI.Account.GetSessionString('OnboardingAPI.Token');
        if (Token) xhttp.setRequestHeader('Authorization', 'Bearer ' + Token);

        xhttp.send(JSON.stringify(RequestPayload));
      });

      return await Request;
    },
    BeforeRequest: function (Payload: any) {
      // const Control = document.getElementById("AgentRequestPayload");
      // if (Control)
      // 	Control.innerText = JSON.stringify(Payload, null, 2);
    },
    AfterResponse: function (Payload: any) {
      // const Control = document.getElementById("AgentResponsePayload");
      // if (Control)
      // 	Control.innerText = JSON.stringify(Payload, null, 2);
    },
    Log: function (Message: string) {
      console.log(new Date().toString() + ': ' + Message);
    },
  },
  ID: {
    CountryCode: async function () {
      const Response = await OnboardingAPI.IO.Request(
        '/ID/CountryCode.ws',
        {},
        {},
      );
      return Response;
    },
    sendVerificationMessage: async function (mobileNumber: string) {
      console.log('mobileNumber === > ', mobileNumber);
      const Response = await OnboardingAPI.IO.Request(
        '/ID/SendVerificationMessage.ws',
        { Nr: mobileNumber },
        {},
      );
      return Response;
    },
    verifyNumber: async function (mobileNumber: string, code: string) {
      const Response = await OnboardingAPI.IO.Request(
        '/ID/VerifyNumber.ws',
        { Nr: mobileNumber, Code: parseInt(code), Test: true },
        {},
      );
      return Response;
    },
    sendEmailVerificationMessage: async function (mobileNumber: string) {
      const Response = await OnboardingAPI.IO.Request(
        '/ID/SendVerificationMessage.ws',
        { EMail: mobileNumber },
        {},
      );
      return Response;
    },
  },
  Account: {
    Utf8: new TextEncoder('windows-1252', {
      NONSTANDARD_allowLegacyEncoding: true,
    }),
    Base64Alphabet: [
      'A',
      'B',
      'C',
      'D',
      'E',
      'F',
      'G',
      'H',
      'I',
      'J',
      'K',
      'L',
      'M',
      'N',
      'O',
      'P',
      'Q',
      'R',
      'S',
      'T',
      'U',
      'V',
      'W',
      'X',
      'Y',
      'Z',
      'a',
      'b',
      'c',
      'd',
      'e',
      'f',
      'g',
      'h',
      'i',
      'j',
      'k',
      'l',
      'm',
      'n',
      'o',
      'p',
      'q',
      'r',
      's',
      't',
      'u',
      'v',
      'w',
      'x',
      'y',
      'z',
      '0',
      '1',
      '2',
      '3',
      '4',
      '5',
      '6',
      '7',
      '8',
      '9',
      '+',
      '/',
    ],
    getRandomValues: function (lenth: any) {
      const char =
        'AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz1234567890';
      const random = Array.from(
        { length: lenth },
        () => char[Math.floor(Math.random() * char.length)]
      );
      return random.join('');
    },
    Base64Encode: function (Data: string | any[]) {
      let Result = '';
      let i;
      const c = Data.length;

      for (i = 2; i < c; i += 3) {
        Result += this.Base64Alphabet[Data[i - 2] >> 2];
        Result +=
          this.Base64Alphabet[((Data[i - 2] & 0x03) << 4) | (Data[i - 1] >> 4)];
        Result +=
          this.Base64Alphabet[((Data[i - 1] & 0x0f) << 2) | (Data[i] >> 6)];
        Result += this.Base64Alphabet[Data[i] & 0x3f];
      }

      if (i === c) {
        Result += this.Base64Alphabet[Data[i - 2] >> 2];
        Result +=
          this.Base64Alphabet[((Data[i - 2] & 0x03) << 4) | (Data[i - 1] >> 4)];
        Result += this.Base64Alphabet[(Data[i - 1] & 0x0f) << 2];
        Result += '=';
      } else if (i === c + 1) {
        Result += this.Base64Alphabet[Data[i - 2] >> 2];
        Result += this.Base64Alphabet[(Data[i - 2] & 0x03) << 4];
        Result += '==';
      }

      return Result;
    },
    XmlEncode: function (s: string) {
      return s
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&apos;')
        .replace(/\n/g, '&#10;')
        .replace(/\r/g, '&#13;')
        .replace(/\t/g, '&#9;')
        .replace(/\v/g, '&#11;')
        .replace(/\f/g, '&#12;');
    },
    Sign: async function (Key: any, Data: any) {
      return Base64.stringify(hmacSHA256(Data, Key));
    },
    GetSessionString: async function (Name: string) {
      return await retrieveUserSession(Name);
    },
    SetSessionString: async function (Name: string, Value: any) {
      return await storeUserSession(Name, Value);
    },
    RemoveSessionValue: async function (Name: string) {
      return await removeUserSession(Name);
    },
    GetSessionInt: function (Name: any) {
      const s = this.GetSessionString(Name);
      if (s) return parseInt(s);
      else return null;
    },
    SetSessionInt: function (Name: any, Value: { toString: () => any }) {
      this.SetSessionString(Name, Value.toString());
    },
    RefreshToken: async function () {
      OnboardingAPI.Account.RemoveSessionValue('OnboardingAPI.RefreshTimer');
      OnboardingAPI.Account.RemoveSessionValue('OnboardingAPI.RefreshTimerElapses');
      OnboardingAPI.Account.RemoveSessionValue('OnboardingAPI.RefreshTimerExpires');

      const Seconds = OnboardingAPI.Account.GetSessionInt('OnboardingAPI.Seconds');
      if (Seconds) OnboardingAPI.Account.Refresh(Seconds, true);
    },
    RestartActiveSession: function () {
      const Token = this.GetSessionString('OnboardingAPI.Token');
      const Seconds = this.GetSessionInt('OnboardingAPI.Seconds');

      if (Token && Seconds) {
        OnboardingAPI.IO.Log('Checking last session.');
        this.CheckSessionToken(Token, Seconds, false);
      } else OnboardingAPI.IO.Log('No session found.');
    },
    CheckSessionToken: function (Token: any, Seconds: number, NewToken: any) {
      const OldTimer = this.GetSessionInt('OnboardingAPI.RefreshTimer');
      const Elapses = this.GetSessionInt('OnboardingAPI.RefreshTimerElapses');
      const Expires = this.GetSessionInt('OnboardingAPI.RefreshTimerExpires');
      const Now = Math.round(Date.now() / 1000);

      if (!OldTimer || !Expires || !Elapses) {
        if (NewToken) OnboardingAPI.IO.Log('Saving new session token.');
        else OnboardingAPI.IO.Log('Resaving session token.');

        this.SaveSessionToken(Token, Seconds, Math.round(Seconds / 2));
      } else if (Expires > Now) {
        if (Elapses <= Now) {
          if (NewToken) {
            OnboardingAPI.IO.Log('New token replaces old token.');
            this.SaveSessionToken(Token, Seconds, Math.round(Seconds / 2));
          } else {
            OnboardingAPI.IO.Log(
              'Token elapsed, but not expired. Refreshing token.'
            );
            this.RefreshToken();
          }
        } else {
          if (NewToken)
            OnboardingAPI.IO.Log(
              'Saving new session token, using previous session timer.'
            );
          else OnboardingAPI.IO.Log('Restarting previous session token.');

          this.SaveSessionToken(Token, Seconds, Elapses - Now);
        }
      } else if (NewToken) {
        OnboardingAPI.IO.Log('Saving new session token.');
        this.SaveSessionToken(Token, Seconds, Math.round(Seconds / 2));
      } else OnboardingAPI.IO.Log('Obsolete session token.');
    },
    SaveSessionToken: function (Token: any, Seconds: number, Next: number) {
      const OldTimer = this.GetSessionInt('OnboardingAPI.RefreshTimer');
      if (OldTimer) {
        OnboardingAPI.IO.Log('Stopping previous session timer.');
        clearTimeout(OldTimer);
      }

      const Now = Math.round(Date.now() / 1000);

      this.SetSessionString('OnboardingAPI.Token', Token);
      this.SetSessionInt('OnboardingAPI.Seconds', Seconds);
      this.SetSessionInt("OnboardingAPI.RefreshTimer", setTimeout(this.RefreshToken, 1000 * Next));
      this.SetSessionInt('OnboardingAPI.RefreshTimerElapses', Now + Next);
      this.SetSessionInt('OnboardingAPI.RefreshTimerExpires', Now + Seconds);

      OnboardingAPI.IO.Log(
        'Resetting session timer to ' +
          Next +
          's, with token life cycle of ' +
          Seconds +
          's'
      );
    },
    Create: async function (
      UserName: string,
      EMail: string,
      Password: string
      // ApiKey?: string,
      // Secret?: any,
      // Seconds?: number
    ) {
      const Nonce = OnboardingAPI.Account.getRandomValues(32);
      const s =
        UserName +
        ':' +
        host +
        ':' +
        EMail +
        ':' +
        Password +
        ':' +
        ApiKey +
        ':' +
        Nonce;
      const Response = await OnboardingAPI.IO.Request('/Agent/Account/Create', {
        userName: UserName,
        eMail: EMail,
        password: Password,
        apiKey: ApiKey,
        nonce: Nonce,
        signature: await this.Sign(Secret, s),
        seconds: Seconds,
      });

      this.SetSessionString('OnboardingAPI.UserName', UserName);
      this.SaveSessionToken(Response.jwt, Seconds, Math.round(Seconds / 2));

      return Response;
    },
    GetSessionToken: async function () {
      const Response = await OnboardingAPI.IO.Request(
        '/Agent/Account/GetSessionToken',
        {}
      );

      this.CheckSessionToken(
        Response.AccountCreated.jwt,
        Response.seconds,
        true
      );
    },
    VerifyEMail: async function (EMail: any, Code: any) {
      const Result = await OnboardingAPI.IO.Request('/Agent/Account/VerifyEMail', {
        eMail: EMail,
        code: Code,
      });

      this.SetSessionString('OnboardingAPI.UserName', Result.userName);

      return Result;
    },
    Login: async function (UserName: string, Password: any, Seconds: number) {
      const Nonce = this.getRandomValues(32);
      const s = UserName + ':' + host + ':' + Nonce;
      let raw = {
        userName: UserName,
        nonce: Nonce,
        signature: await this.Sign(Password, s),
        seconds: 3500,
      };
      const Response = await OnboardingAPI.IO.Request('/Agent/Account/Login', raw);

      this.SetSessionString('OnboardingAPI.UserName', UserName);
      this.SaveSessionToken(Response.jwt, Seconds, Math.round(Seconds / 2));

      return Response;
    },
    Refresh: async function (Seconds: number, Internal: any) {
      OnboardingAPI.IO.Log('Requesting a refresh of Agent API session token.');

      const Response = await OnboardingAPI.IO.Request(
        '/Agent/Account/Refresh',
        {
          seconds: Seconds,
        },
        Internal
      );

      this.SaveSessionToken(Response.jwt, Seconds, Math.round(Seconds / 2));

      return Response;
    },
    Logout: async function () {
      const OldTimer = this.GetSessionInt('OnboardingAPI.RefreshTimer');
      if (OldTimer) {
        OnboardingAPI.IO.Log('Stopping session timer.');
        clearTimeout(OldTimer);
      }

      this.RemoveSessionValue('OnboardingAPI.UserName');
      this.RemoveSessionValue('OnboardingAPI.Seconds');
      this.RemoveSessionValue('OnboardingAPI.RefreshTimer');
      this.RemoveSessionValue('OnboardingAPI.RefreshTimerElapses');
      this.RemoveSessionValue('OnboardingAPI.RefreshTimerExpires');

      const Response = await OnboardingAPI.IO.Request('/Agent/Account/Logout', {});

      this.RemoveSessionValue('OnboardingAPI.Token');

      return Response;
    },
  },
};

OnboardingAPI.Account.RestartActiveSession();
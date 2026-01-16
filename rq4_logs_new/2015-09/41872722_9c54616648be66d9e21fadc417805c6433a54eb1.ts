/// <reference path="../typings/google-apps-script/google-apps-script.forms.d.ts"/>
/// <reference path="../typings/google-apps-script/google-apps-script.url-fetch.d.ts"/>
/// <reference path="../typings/google-apps-script/google-apps-script.mail.d.ts"/>
/// <reference path="../typings/google-apps-script/google-apps-script.properties.d.ts"/>
/// <reference path="../typings/google-apps-script/google-apps-script.base.d.ts"/>

// https://sendgrid.com/docs/API_Reference/Marketing_Emails_API/index.html

import FormResponse = GoogleAppsScript.Forms.FormResponse;

const list_name = PropertiesService.getScriptProperties().getProperty("list_name") + "";
const api_user = PropertiesService.getScriptProperties().getProperty("api_user");
const api_key = PropertiesService.getScriptProperties().getProperty("api_key");
const alert_to = PropertiesService.getScriptProperties().getProperty("alert_to") + "";
if (!list_name || !api_user || !api_key || !alert_to) {
    throw new Error("Require properties are not specified!");
}
if (/\+/.test(alert_to)) {
    throw new Error("MailApp.sendEmail does not support '+' alias of Gmail.");
}

function testSend() {
    var email = "test@testtest";
    addEmail(list_name, { name: email, email: email });
}

/**
 * メールアドレスをSendGridの送信先リストに追加する
 */
function addEmail(list: string, data: { name: string, email: string }) {
    const url = 'https://api.sendgrid.com/api/newsletter/lists/email/add.json';
    let params = {
        method: "post",
        payload: {
            list: list,
            data: JSON.stringify(data),
            api_user: api_user,
            api_key: api_key
        }
    };
    Logger.log("------ SendGrid API request ------");
    Logger.log(params);
    let res = UrlFetchApp.fetch(url, params);
    Logger.log("------ SendGrid API response ------");
    Logger.log(res);
    let json = JSON.parse(res.getContentText().toString());
    // inserted=0 は許容する。だたの二重登録なので。
    if (res.getResponseCode() !== 200 || json["inserted"] > 1) {
        throw new AddMailError(res.getResponseCode(), res.getContentText().toString());
    }
}

/**
 * Formが送信されたイベントのハンドラ
 */
function handleFormSubmitted(e: { response: FormResponse }) {
    Logger.log(e);
    var item = e.response.getItemResponses()[0]; // メルアド欄のみ
    var email = item.getResponse().toString();
    try {
        addEmail(list_name, { name: email, email: email });
    } catch (e) {
        Logger.log("------ Error occued! ------");
        let error_detail = utilToString(e);
        if (!error_detail) {
            error_detail = e.toString();
        }
        Logger.log(error_detail);
        // 失敗をメールで通知
        MailApp.sendEmail({
            to: alert_to,
            subject: `[ALERT]${list_name}`,
            body: `メールアドレスの登録に失敗\n${maskEmail(email) }\n\n${error_detail}`
        });
    }
    function maskEmail(email: string) {
        return email.split('@').map((value, index, array) => {
            let maskLength = (value.length / 2) | 0;
            return value.substr(0, value.length - maskLength) + strRepeat("*", maskLength);
        }).join("@");
    }
    function strRepeat(str: string, length: number) {
        let result = "";
        for (let i = 0; i < length; i++) {
            result += str;
        }
        return result;
    }
    function utilToString(data:any) {
        let str = "";
        for(let key in data) {
            str += `${key}=${data[key]}\n`
        }
        return str;
    }
}

class AddMailError implements Error {
    public name = "AddMailError";
    public message:string;
    constructor(status_code: number, body: string) {
        this.message = `status_code: ${status_code}\nbody: ${body}`;
    }
}
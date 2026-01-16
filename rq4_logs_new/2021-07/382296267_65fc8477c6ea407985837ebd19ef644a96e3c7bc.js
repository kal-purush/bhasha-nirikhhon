/**
 * Created by jesusxiong on 2021/6/3
 */

const axios = require('axios');
const wecomCrypto = require('@wecom/crypto');
const xml2js = require('xml2js');

// 验证服务
function botReceiverVerify(event) {
    // 1. 获取验证的加密参数
    const { echostr } = event.queryStringParameters;
    // 2. TODO 对msg_signature进行校验
    // 3. 解密消息
    const reqMsg = wecomCrypto.decrypt(process.env.encodingAESKey, echostr);
    // 4. 返回验证信息
    return reqMsg.message;
}

// 响应消息
async function botReceiverHandler(event) {
    // 1. 获取加密数据
    let encrypt;
    xml2js.parseString(event.body, function (err, result) {
        encrypt = result.xml.Encrypt[0];
    });
    // 2. TODO 对msg_signature进行校验
    // 3. 解密消息
    const reqMsg = wecomCrypto.decrypt(process.env.encodingAESKey, encrypt);
    // 4. 解密结果
    let message;
    xml2js.parseString(reqMsg.message, function (err, result) {
        message = result.xml;
    });
    // 5. 响应动作
    if (message.ChatId) {
        await axios.post(process.env.botWebHookUrl, {
            chatid: message.ChatId,
            msgtype: 'text',
            text: {
                content: 'hello, ' + JSON.stringify(message.From),
            },
        });
    }
}

exports.main = async (event) => {
    console.log('main event', event)
    if (event.httpMethod === 'GET') {
        return botReceiverVerify(event)
    } else {
        try {
            return await botReceiverHandler(event);
        } catch (e) {
            console.log(e);
            return { ret: false }
        }
    }
}
//查询所有城市，并且把所有城市电梯拉取出来广播

import Parse from 'parse/node';
import rp from 'request-promise';

export default async function (res) {
    try {
        let access = await rp({
            method: 'POST',
            uri: `https://kc3ip-userauth.mychinabluemix.net/oidc/endpoint/OP/token`,
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
            },
            body: "grant_type=password&username=" + "kc3ui2" + "&password=" + "kone365" + "&scope=openid profile email&client_secret=g$7Ewes*_usw-caP5wr6&client_id=haiforce",
        })
        console.log("dddd",access)
        let Role = Parse.Object.extend(Parse.Role)
        let query = new Parse.Query(Role);
        let find = await query.find()
        let resultArr = []
        for (let i = 0; i < find.length; i++) {
            let result = await find[i].relation("detail").query().find()
            if (result[0]) {
                resultArr.push(result[0].get("nickName"))
            } else {
                continue
            }
        }
        let infoArr = []
        for (let i = 0; i < find.length; i++) {
            let temp = await rp({
                method: 'POST',
                uri: `http://kc3ip-equipment.mychinabluemix.net/realtime/rest/equipment/statusRefresh`,
                headers: {
                    'content-type': "application/iot.kone.v3+json",
                    'access_token': JSON.parse(access).access_token
                },
                body: {
                    "plantSection": ["CNB"],
                    "startTime": "1970-01-01T00:00:00",
                    "start": 1,
                    "limit": 100
                },
                json: true
            })

            infoArr = infoArr.concat(temp)
        }

        res.send(infoArr)
    } catch (error) {
        res.send(error)
    }
}
'use strict';
const AWS = require('aws-sdk')
const sns = new AWS.SNS()

const simpleTest = require("./test/simpleTest")

class Supervise{

    constructor(topicArn)
    {
        this.topicArn = topicArn
    }

    run(event, context, cb)
    {
        let pList = [];
        const config = require("json!yaml!../entries.yaml")

        config.entries.simple.forEach((url)=>{
            pList.push(simpleTest("http://chatbox-inc.com" + url).request())
            pList.push(simpleTest("https://chatbox-inc.com" + url).request())
        })
        config.entries.simple404.forEach((url)=>{
            pList.push(simpleTest("http://chatbox-inc.com" + url,404).request())
            pList.push(simpleTest("https://chatbox-inc.com" + url,404).request())
        })

        const done = this.done(cb)

        Promise.all(pList).then((resultAll)=>{
            let errors = []
            resultAll.forEach((visor)=>{
                const error = visor.getErrors()
                if(error.length !== 0){
                    errors.push(visor.name)
                    error.forEach((e)=>{
                        console.log(e.message)
                    })
                }
            })
            return errors
        }).then((errors)=>{
            if(errors.length === 0){
                console.log("all test have passed")
                done()
            }else{
                const Message = this.message(errors)
                const TopicArn = this.topicArn

                console.log("error")
                sns.publish({
                    Message,
                    Subject: "SLS BACKLOG WEBHOOK APPLICATION",
                    TopicArn
                }, done)
            }
        })
    }

    message(errors)
    {
        let text = "外形監視項目でいくつかのテストに失敗しました。\n"
        errors.forEach((item)=>{
            text = text + item + "\n"
        })
        return JSON.stringify({
            channel: "#excom",
            text
        })
    }

    done(cb) {
        return (err, result) => cb(null, {
            statusCode: err ? '500' : '200',
            body: err ? err.message : JSON.stringify(result),
            headers: {
                'Content-Type': 'application/json',
            },
        })
    }



}



module.exports = Supervise
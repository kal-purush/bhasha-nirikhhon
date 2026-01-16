const express = require('express')
const http = require('http')
const socketio = require('socket.io')
const { sdk } = require('symbl-node')
const streamBuffers = require('stream-buffers')
const path = require('path')


const port = 4000
const app = express()

const server = http.createServer(app);
const io = new socketio.Server();
io.attach(server);
app.use("/public", express.static(path.join(__dirname, "public")));

app.get('/home', (req, res, next) => {
    return res.sendFile(path.join(process.cwd(), "public", "index.html"));

})
const APP_ID = '444a51633345706b4b65444d754b4133716e644d424d753557614130586e4562'
const APP_SECRET = '676131756752464d7a785a6b6a61514c5f6677313152394d454f6f412d6e34386e335575575979696c59574237576845775354325033306d6752617769583157'


const getConnection = async() => {
    try {
        const connection = await sdk.startRealtimeRequest({
            id: '19898910101010',
            config: {
                meetingTitle: 'My Tng',
                confidenceThreshold: 0.6,
                timezoneOffset: 480, // Offset in minutes from UTC
                languageCode: 'en-US',
                sampleRateHertz: 14000
            },
            handlers: {
                /**
                 * This will return live speech-to-text transcription of the call.
                 */
                onSpeechDetected: (data) => {
                    console.log(data)
                    if (data) {
                        const { punctuated } = data
                        console.log('Live: ', punctuated && punctuated.transcript)
                        console.log('Here');

                    }
                    console.log('onSpeechDetected ', JSON.stringify(data, null, 2));
                    socket.emit('symbl-data', data)

                },
                /**
                 * When processed messages are available, this callback will be called.
                 */
                onMessageResponse: (data) => {
                    console.log('onInsightResponse', JSON.stringify(data, null, 2))
                    socket.emit('symbl-data', data)
                },
                /**
                 * When Symbl detects an insight, this callback will be called.
                 */
                onInsightResponse: (data) => {
                    console.log('onInsightResponse', JSON.stringify(data, null, 2))
                    socket.emit('symbl-data', data)

                },
                /**
                 * When Symbl detects a topic, this callback will be called.
                 */
                onTopicResponse: (data) => {
                    console.log('onTopicResponse', JSON.stringify(data, null, 2))
                    socket.emit('symbl-data', data)

                }
            }
        });
        return connection
    } catch (error) {
        console.log({ error: error })
        return
    }
}
io.once("connection", async(socket) => {
    sdk.init({
            // APP_ID and APP_SECRET come from the Symbl Platform: https://platform.symbl.ai
            appId: APP_ID,
            appSecret: APP_SECRET,
            basePath: 'https://api.symbl.ai'
        })
        .then(() =>
            console.log('SDK Initialized.')
        )
        .catch(err => console.error('Error in initialization.', err));

    let sampleRate = 48000
    socket.on('start', async(data) => {
        sampleRate = data.sampleRate
        console.log(`Sample Rate: ${sampleRate}`)

    })


    socket.on('send_pcm', async(data) => {
        const connection = await getConnection()
        if (connection) {
            var myReadableStreamBuffer = new streamBuffers.ReadableStreamBuffer({
                frequency: 10, // in milliseconds.
                chunkSize: 20048 // in bytes.
            });

            // With a buffer
            myReadableStreamBuffer.put(data);
            myReadableStreamBuffer.on('data', function(data) {});
            connection.sendAudio(data)

        } else {
            socket.emit('symbl-data', { mesg: "Error Reteriving Data" })

        }




    })

    socket.on('stop', (data, ack) => {
        socket.disconnect(true)
        console.log('Connection Stopped')
        ack({ filename: "Stoppeed Connection" })
    })
})

// Convert byte array to Float32Array
const toF32Array = (buf) => {
    const buffer = new ArrayBuffer(buf.length)
    const view = new Uint8Array(buffer)
    for (var i = 0; i < buf.length; i++) {
        view[i] = buf[i]
    }
    return new Float32Array(buffer)
}












server.listen(port, () => {
    console.log(`Listening to Port ${port}`)
})
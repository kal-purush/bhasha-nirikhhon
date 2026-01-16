import { ExpoPushClient } from "../main/clients/ExpoPushClient"

/* function generateMessages(nrOfMessages: number){
    let messages: any[] = [];
    for(let i = 0; i < nrOfMessages; i++){
        messages.push({
            title: "test",
            body: "test",
            data: {
                test: "test"
            }
        });
    }
} */


describe('Expo Push Client test', () => {
    
    const client = new ExpoPushClient();

    test('Push to clients with invalid tokens', () => {
        const recipients = ['invalidToken1', 'invalidToken2'];
        const messageData = {
            title: "test",
            body: "test",
            data: {
                test: "test"
            }
        };
        expect(() => {
            client.pushToClients(recipients, messageData)
        }).toThrow();
    });

    test('Push to clients with valid tokens', () => {
        const recepients = [
            "ExponentPushToken[KWMhzYAFDTc8hT4ymrg0rg]"
        ]
        const messageData = {
            //to: "ExponentPushToken[KWMhzYAFDTc8hT4ymrg0rg]",
            title: "test",
            body: "test",
            data: {
                test: "test"
            }
        };
        client.pushToClients(recepients, messageData)
    })


})
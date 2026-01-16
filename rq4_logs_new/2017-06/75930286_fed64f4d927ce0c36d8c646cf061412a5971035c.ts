export class CreateMatchdayProviderMock {
    dataTemplate: Array<any>;
    dataTeam: Array<any>;
    playerArray: Array<any>;
    counter: any;

    constructor() {

    }

    setTemplates(): Promise<{ status: string }> {
        return new Promise((resolve) => {
            var templateArray = [
                {
                    club: "Testclub",
                    street: "Teststrasse 3",
                    zipcode: "11111",
                }
            ]
            this.dataTemplate = templateArray;
            if (this.dataTemplate != undefined) {
                resolve("success");
            }
        }).catch((reject) => {
            reject("failed");
        })
    }

    setTeams(): Promise<{ status: string }> {
        return new Promise((resolve) => {
            var teams = [
                {

                    id: "abcdefgh",
                    ageLimit: "15",
                    type: "1",
                    name: "Team 1",
                    sclass: "S1",
                    players: {
                        0: "12345"
                    }

                },
                {

                    id: "ijklmnop",
                    ageLimit: "0",
                    type: "0",
                    name: "Team 2",
                    sclass: "S2",
                    players: {
                        0: "54321"
                    }

                }]
            this.dataTeam = teams;
            if (this.dataTeam != undefined) {
                resolve("success");
            }
        }).catch((reject) => {
            reject("failed");
        })


    }

    setPlayer(Utilities, gameItem, acceptedArray, pendingArray, declinedArray): Promise<{ status: string }> {
        return new Promise((resolve) => {
            var playerArray = [
                {
                    age: "27",
                    id: "1111",
                    birthday: "1990-01-01",
                    email: "test1@test.de",
                    firstname: "Max",
                    gender: "m",
                    isPlayer: true,
                    isTrainer: false,
                    lastname: "Mustermann",
                    picUrl: "",
                    state: 0,
                    team: "10",
                    accepted: true,
                    pending: false,
                    declined: false,
                    deleted: false,
                    isMainTeam: true
                },
                {
                    age: "26",
                    id: "2222",
                    birthday: "1991-01-01",
                    email: "test2@test.de",
                    firstname: "Erika",
                    gender: "f",
                    isPlayer: true,
                    isTrainer: true,
                    lastname: "Musterfrau",
                    picUrl: "",
                    state: 0,
                    team: "10",
                    accepted: false,
                    pending: false,
                    declined: true,
                    deleted: false,
                    isMainTeam: true
                },
                {
                    age: "25",
                    id: "3333",
                    birthday: "1989-01-01",
                    email: "test3@test.de",
                    firstname: "Test",
                    gender: "m",
                    isPlayer: true,
                    isTrainer: false,
                    lastname: "Person",
                    picUrl: "",
                    state: 0,
                    team: "12",
                    accepted: false,
                    pending: true,
                    declined: false,
                    deleted: false,
                    isMainTeam: false
                }

            ]
            for (let i in playerArray){
                this.counter++;
            }
            this.playerArray = playerArray;
            if (this.playerArray != undefined) {
                resolve("success");
            }
        }).catch((reject) => {
            reject("failed");
        })
    }
}
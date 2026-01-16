const Command = require('./command');
const { Message, OpType, Location, Profile } = require('../curve-thrift/line_types');

class LINE extends Command {
    constructor() {
        super();
        this.receiverID = '';
        this.checkReader = [];
        this.stateStatus = {
            cancel: 0,
            kick: 0,
        };
        this.messages;
        this.payload;
        this.stateUpload =  {
                file: '',
                name: '',
                group: '',
                sender: ''
            }
    }


    get myBot() {
        const bot = ['u813e54635fa8ca8c016090e933582652','u88da02958609ecc859dcc35703260360','u69e21a482cfd20d69e64c61211981f9a','u82baf31849e833be723c3e02baa8343d','u32fd5da49b4de7889329b6d34cabd76f','uee9e80bb60a2643670e51eb5e36b25e9','u5062435f8fbb06f189dc92a2ed9526ad','ud9997652103dfbde0ce50d6262d006f2','u8d67d89eeade3917f713d3d3f22df048','u6d007f570d0c94893fe9480ad2681229','uac2caa69eedc8b0cbd16dc67f91e1774','u773cc1cff3e0d57255812b0873fd55a4','u965d3beb7171dd4c86e1f7c1563ec0a6'];
        return bot; 
    }

    isAdminOrBot(param) {
        return this.myBot.includes(param);
    }

    getOprationType(operations) {
        for (let key in OpType) {
            if(operations.type == OpType[key]) {
                if(key !== 'NOTIFIED_UPDATE_PROFILE') {
                    console.info(`[* ${operations.type} ] ${key} `);
                }
            }
        }
    }

    poll(operation) {
        if(operation.type == 25 || operation.type == 26) {
            let message = new Message(operation.message);
            this.receiverID = message.to = (operation.message.to === this.myBot[0]) ? operation.message.from : operation.message.to ;
            Object.assign(message,{ ct: operation.createdTime.toString() });
            this.textMessage(message)
        }

        if(operation.type == 13 && this.stateStatus.cancel == 1) {
            this._cancel(operation.param2,operation.param1);
            
        }

        if(operation.type == 11 && !this.isAdminOrBot(operation.param2) && this.stateStatus.qrp == 1) {
            this._kickMember(operation.param1,[operation.param2]);
            this.messages.to = operation.param1;
            this.qrOpenClose();
        }

        if(operation.type == 19) { //ada kick
     {
let nadyaq = new Message();
nadyaq.to = operation.param1;
nadyaq.text = "x_x"
this._client.sendMessage(0,nadyaq);
     }
            // op1 = group nya
            // op2 = yang 'nge' kick
            // op3 = yang 'di' kick

            if(!this.isAdminOrBot(operation.param2)){
                this._kickMember(operation.param1,[operation.param2]);
            } 
            if(!this.isAdminOrBot(operation.param3)){
                this._invite(operation.param1,[operation.param3]);
            }

        }

if(operation.type == 11) { //bukattupQR
     {
let nadyasayang = new Message();
nadyasayang.to = operation.param1;
nadyasayang.text = "-__-"
this._client.sendMessage(0,nadyasayang);
     }
            if(!this.isAdminOrBot(operation.param2)){
                this._kickMember(operation.param1,[operation.param2]);
            } 
}

if(operation.type == 15) { //leave grup
     {
let nadyasayang = new Message();
nadyasayang.to = operation.param1;
nadyasayang.text = "o_o"

this._client.sendMessage(0,nadyasayang);
     }
  this._invite(operation.param1,[operation.param2]);
}

if(operation.type == 17) { //joingroup
let nadyaq = new Message();
nadyaq.to = operation.param1;
nadyaq.text = "Selamat datang, semoga betah disini..."

this._client.sendMessage(0,nadyaq);
}

if(operation.type == 32) { //adaygbatalin
let nadyaq = new Message();
nadyaq.to = operation.param1;
nadyaq.text = "b_d"

this._client.sendMessage(0,nadyaq);
     }

        if(operation.type == 55){ //ada reader
            const idx = this.checkReader.findIndex((v) => {
                if(v.group == operation.param1) {
                    return v
                }
            })
            if(this.checkReader.length < 1 || idx == -1) {
                this.checkReader.push({ group: operation.param1, users: [operation.param2], timeSeen: [operation.param3] });
            } else {
                for (var i = 0; i < this.checkReader.length; i++) {
                    if(this.checkReader[i].group == operation.param1) {
                        if(!this.checkReader[i].users.includes(operation.param2)) {
                            this.checkReader[i].users.push(operation.param2);
                            this.checkReader[i].timeSeen.push(operation.param3);
                        }
                    }
                }
            }
        }


        if(operation.type == 5) { // diadd
let nadyaq = new Message();
nadyaq.to = operation.param1;
nadyaq.text = "Makasih sudah di add ^_^"
this._client.sendMessage(0,nadyaq);
}

        if(operation.type == 13) { // diinvite
                this._acceptGroupInvitation(operation.param1);
let nadyaq = new Message();
nadyaq.to = operation.param1;
nadyaq.text = "Makasih sudah diinvite, salken semua..."

this._client.sendMessage(0,nadyaq);
}
        this.getOprationType(operation);
    }

    command(msg, reply) {
        if(this.messages.text !== null) {
            if(this.messages.text === msg.trim()) {
                if(typeof reply === 'function') {
                    reply();
                    return;
                }
                if(Array.isArray(reply)) {
                    reply.map((v) => {
                        this._sendMessage(this.messages, v);
                    })
                    return;
                }
                return this._sendMessage(this.messages, reply);
            }
        }
    }

    async textMessage(messages) {
        this.messages = messages;
        let payload = (this.messages.text !== null) ? this.messages.text.split(' ').splice(1).join(' ') : '' ;
        let receiver = messages.to;
        let sender = messages.from;
        

        this.command('Me', this.getProfile.bind(this));
        this.command('Status', `Your Status: ${JSON.stringify(this.stateStatus)}`);
        this.command(`Left ${payload}`, this.leftGroupByName.bind(this));
        this.command('Speed', this.getSpeed.bind(this));
        this.command('Kernel', this.checkKernel.bind(this));
        this.command(`Kick ${payload}`, this.OnOff.bind(this));
        this.command(`Cancel ${payload}`, this.OnOff.bind(this));
        this.command(`Qrp ${payload}`, this.OnOff.bind(this));
        this.command(`Cipok ${payload}`,this.kickAll.bind(this));
        this.command(`Cancelall ${payload}`, this.cancelMember.bind(this));
        this.command(`Set`,this.setReader.bind(this));
        this.command(`set`,this.setReaderr.bind(this));
        this.command(`Setpoint`,this.setReaderrr.bind(this));
        this.command(`setpoint`,this.setReaderrrr.bind(this));
        this.command(`Recheck`,this.rechecks.bind(this));
        this.command(`recheck`,this.recheckss.bind(this));
        this.command(`Check`,this.rechecksss.bind(this));
        this.command(`check`,this.recheckssss.bind(this));
        this.command(`check sider`,this.recheck.bind(this));
        this.command(`Clearall`,this.clearall.bind(this));
        this.command(`Clear`,this.clear.bind(this));
        this.command(`clear`,this.clear1.bind(this));
        this.command(`Reset`,this.reset.bind(this));
        this.command(`reset`,this.reset1.bind(this));
        this.command('Myid',`Your ID: ${messages.from}`)
        this.command(`ip ${payload}`,this.checkIP.bind(this))
        this.command(`Ig ${payload}`,this.checkIG.bind(this))
        this.command(`Qr ${payload}`,this.qrOpenClose.bind(this))
        this.command(`Joinqr ${payload}`,this.joinQr.bind(this));
        this.command(`Spam ${payload}`,this.spam2.bind(this));
        this.command(`Spamgroup ${payload}`,this.spamGroup.bind(this));
        this.command(`Creator`,this.creator.bind(this));
        this.command(`List admin`,this.list.bind(this));
        this.command(`Admin1`,this.admin1.bind(this));
        this.command(`Admin2`,this.admin2.bind(this));
        this.command(`Admin3`,this.admin3.bind(this));
        this.command(`Admin4`,this.admin4.bind(this));
        this.command(`Admin5`,this.admin5.bind(this));
        this.command(`Admin6`,this.admin6.bind(this));
        this.command(`Admin7`,this.admin7.bind(this));
        this.command(`Admin8`,this.admin8.bind(this));
        this.command(`Admin9`,this.admin9.bind(this));
        this.command(`Admin10`,this.admin10.bind(this));
        this.command(`Admin11`,this.admin11.bind(this));
        this.command(`Admin12`,this.admin12.bind(this));
        this.command(`Admin13`,this.admin13.bind(this));
        this.command(`Admin14`,this.admin14.bind(this));
        this.command(`Admin15`,this.admin15.bind(this));
        this.command(`Pap ${payload}`,this.searchLocalImage.bind(this));
        this.command(`Upload ${payload}`,this.prepareUpload.bind(this));
        this.command(`Desah`,this.tagall.bind(this));
        this.command(`Tagall`,this.tagall1.bind(this));
        this.command(`Help`,this.help.bind(this));
        this.command(`/help`,this.help1.bind(this));
        this.command(`Keyword`,this.help2.bind(this));
        this.command(`Key`,this.help3.bind(this));
        this.command(`Info kick`,this.infokick.bind(this));
        this.command(`List lagu1`,this.listlagu1.bind(this));
        this.command(`List lagu2`,this.listlagu2.bind(this));
        this.command(`Keluar`,this.keluar.bind(this));
        this.command(`Batal`,this.batal.bind(this));


        if(messages.contentType == 13) {
            messages.contentType = 0;
            if(!this.isAdminOrBot(messages.contentMetadata.mid)) {
                this._sendMessage(messages,messages.contentMetadata.mid);
            }
            return;
        }

  if (messages.text == 'Gift'){
        messages.contentType = 0;
       this._sendMessage(messages, "gift sent",messages.contentMetadata=null,messages.contentType=9);
     }


  if (messages.text == 'Silahkan'){
               await this._sendMessage(messages,'-_-');
     {
this._leaveGroup(this.messages.to);
     }
     }

        if(this.stateUpload.group == messages.to && [1,2,3].includes(messages.contentType)) {
            if(sender === this.stateUpload.sender) {
                this.doUpload(messages);
                return;
            } else {
                messages.contentType = 0;
                this._sendMessage(messages,'Wrong Sender !! Reseted');
            }
            this.resetStateUpload();
            return;
        }

        // if(cmd == 'lirik') {
        //     let lyrics = await this._searchLyrics(payload);
        //     this._sendMessage(seq,lyrics);
        // }

    }

}

module.exports = LINE;
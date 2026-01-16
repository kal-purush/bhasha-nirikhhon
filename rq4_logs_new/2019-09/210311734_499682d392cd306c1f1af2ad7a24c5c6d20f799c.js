const AmoCRM = require( 'amocrm-js' );
var AWS = require("aws-sdk");

AWS.config.update({region: 'eu-central-1'});

var dynamoDb = new AWS.DynamoDB.DocumentClient();

//Get array with objects from database
const getDataDynamoDB = new Promise((resolve, reject) => {
    dynamoDb.scan({TableName: "client"}).promise()
                    .then(data => resolve(data.Items));
});

const crm = new AmoCRM({
    domain: 'acrylnod', 
    auth: {
        login: process.env.auth_login,
        hash: process.env.auth_hash
    }
});

const addCRM =  data => {
       //Search TxAssetID in CRM 
       crm.Lead.find({query: data.TxAssetID})
         .then((res) => { 
            
            if ( res.length == 0 ){
                console.log(data.TxAssetID+' = ' +res.length )
                //add contact
                crm.Contact.insert([{
                    name: data.customName,
                    responsible_user_id: "3316417",
                    tags: "купить майнер",
                    custom_fields: [
                        { 
                            id: "543953",
                            name: 'Телефон',
                            code: 'PHONE',
                            values: [{
                                value: data.phone,
                                enum: 907777
                            }],
                            is_system: true 
                        },
                        { 
                            id: "543955",
                            name: 'Email',
                            code: 'EMAIL',
                            values: [{
                                value: data.email,
                                enum: 907789
                            }],
                            is_system: true 
                        }
                    ]
                }])
                .then(result => {
                    //add lead
                    crm.Lead.insert([{
                        name: 'Покупка майнеров с client`а',
                        status_id: "29857402",
                        responsible_user_id: "55014691",
                        sale: 115,
                        tags: "buy_miner",
                        contacts_id: [
                            result._response._embedded.items[0].id 
                        ],
                        custom_fields: [
                            {
                                id: 657873,
                                name: "Адрес доставки",
                                values: [{ 
                                    value: data.postCode + ', ' + data.country + ', ' + data.countryState + ', ' + data.sity + ', ' + data.address, 
                                }],
                                is_system: false
                            },
                            {
                                id: 664225,
                                name: "TxAssetID",
                                values: [{ 
                                    value: data.TxAssetID
                                }],
                                is_system: false
                            },
                            {
                                id: 664227,
                                name: "countMiners",
                                values: [{ 
                                    value: data.countMiners
                                }],
                                is_system: false
                            },
                        ]
                    }])
                    .catch(error => console.error('Error with add lead: ', error));;
                })
                .catch(error => console.error('Error with add contact: ', error));
            }else{
                console.log(res.length );
            }
        })   
}

exports.handler = (event, context, callback) => {
     crm.connect().then(() => {
        console.log( `Sign in in portal` );

         getDataDynamoDB.then(result => {   
            result.map(data => addCRM(data));
        });

    })
    .catch( error => {
        console.log( 'Error login: ', error );
    });  
}
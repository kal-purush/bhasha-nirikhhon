var rp = require('request-promise');
const fs = require('fs');

var config = JSON.parse(fs.readFileSync('config.json','utf8'));


function register_agents(amount, sub_domain,distr_Id, agents_dir )
{
     
    if (!fs.existsSync(agents_dir)){
        fs.mkdirSync(agents_dir);
    }
    
    for (var i=0;i<amount;i++){
        var options = {
            method: 'POST',
            uri: `https://ch-${sub_domain}/operations/provision/register`,
            headers : {'X-Auth1': 'kuku', 'Content-Type': 'application/json' },
            json: true, // Automatically stringifies the body to JSON
            resolveWithFullResponse: true,
            family: 4,
            body: {
                "computerName": "TLV-12345"+`${i}`, //{{$timestamp}}
                "agentId" : "",
                "distrId" : distr_Id,
                "goldenImageId" : "",
                "ip" : "10.196.0.8",
                "is64" : true,
                "osType" : 1,
                "osVersion" : "10.13.4",
                "productName" : "",
                "computerSid" : "S-01",
                "productType" : 1,
                "protectionStatus" : 0,
                "trapsVersion" : "5.0.2.1140",
                "userName" : "rgurvits",
                "vdi" : 0
                },
        
            };
        rp(options)
        .then(function (response) {  // export  NODE_TLS_REJECT_UNAUTHORIZED=0
            var xauth = response.caseless.get('X-Auth');
            options.uri = `https://ch-${sub_domain}/operations/provision/confirmation`;
            options.method = 'GET';
            options.headers = { 'X-Auth':`${xauth}` };
            options.body = '';
            options.json = false;
            // Confirmation
                rp(options)
                    .then(function (response) {
                        var auth = response.caseless.get('X-Auth');
                        var file_path = agents_dir + '/' + JSON.parse(response.body)['agentId'];
                        fs.writeFile(file_path,auth, (err) => {  
                            // throws an error, you could also catch it here
                            if (err) throw err;
                        });
                    })
                    .catch(function (err) {
            // GET failed...
                    console.log('\n%%%%%%%%%%% Confimation FAILED!');
                    console.log(`\n%%%%%%%%%%% ${err} FAILED!`);
                    });
        })
        .catch(function (err) {
            console.log(`\n%%%%%%%%%%% ${err} FAILED!`);
            // POST failed...
        });
    }
}

let tenants;

// get tenants per environment from config.json
var firstKey = Object.keys(config.environments)[0];
process.argv[2] === undefined ? tenants = config.environments[firstKey] : tenants = config.environments[process.argv[2]];

tenants.forEach(function(tenant) {
  register_agents(config.agents_to_register,tenant.subdomain,tenant.distId,tenant.agentWH);
});

var http = require('http');
var url = require('url');

var portRegisterServer = 8082;
var usersList = [];
const portInterServer1 = 8000;
const host = "localhost";
const TimeTorestart = 30000;



var interServerRequestHandlerRegister = function(req, res){
    var path = req.url.split('?')[0];
    if(!path || path =='/'){
        res.writeHead(404, {'Content-type': 'application/json'});
        res.end('{message : "page not found"}');
    }else{
        if(req.method == 'POST'){
            if(path == '/register'){
                var infos = {
                    name:"",
                    host:"",
                    port:""
                };
                res.writeHead(200, {'Content-type': 'application/json'});
                req.on('data', function(data){
                    infos = data.toString();
                });
                req.on('end', function(){

                        let UserToRegister = JSON.parse(infos);
                        if(UserToRegister.port!==undefined && !UserToRegister.host!==undefined && UserToRegister.name!=undefined)
                            {
                                let isPresent = usersList.find( user=> (user.host==UserToRegister.host && user.port==UserToRegister.port) || user.name==UserToRegister.name)
                                if(!isPresent){
                                    usersList.push(JSON.parse(infos));                    
                                }
                                let data = JSON.stringify(usersList);
                                isPresent ? res.end(`{"message":"cet utilisateur existe déjà dans le registre"}`) : res.end(`{"message":"Utilisateur connecté", "users":${data}}`.toString());
                            }else{
                                res.end(`{"message":"wrong parameter"}`)
                            }

                    });  

            }else if(path == '/logout'){
                var infos = {
                    name:"",
                    host:"",
                    port:""
                };
                res.writeHead(200, {'Content-type': 'application/json'});
                req.on('data', function(data){
                    infos = data;
                });
                req.on('end', function(){
                    let user = usersList.find(user => user.toString() == infos.toString());
                    let userDelete = false;
                    if(user){
                        let indexOfUserDelete = usersList.indexOf(user);
                        usersList.splice(indexOfUserDelete, 2);
                        userDelete = true;                    
                    }
                    userDelete ? res.end([`L'utilisateur suivant a bien déconnecté`, user].toString()) : res.end("cet utilisateur n'est pas encore connecté");
                });  
            }else{
                // TODO
            }           
        }else{
            res.writeHead(404, {'Content-type': 'application/json'});
            res.end('{message : "page not found"}');
        }
    }
}


function request() {
    if( usersList ){
        usersList.map(user=>{
            let options = {
                hostname: host,
                host:  host+":"+portInterServer1,
                port : portInterServer1,
                method : "POST",
                path: "/ping",
                headers:{
                    host:user.host,
                    port:user.port
                }
            }
            var req = http.request(options, function(res){
                var data = ""
                res.on('data', function(chunk){
                    data += chunk;
                });
                res.on('end', function(){
                    if( res.statusCode=="500"){
                        let userdata = usersList.find(userTofind => userTofind.name == user.name);
                        if(userdata){
                            let indexOfUserDelete = usersList.indexOf(userdata);
                            usersList.splice(indexOfUserDelete, 1);                 
                        }
                    }
                })
            });
            req.on("error", function(e){
                console.log( e )
                console.log( "An error occur...." );
            });
            req.end(JSON.stringify(usersList));
        });
    }
}
setInterval(request, TimeTorestart);
var registerServer = http.createServer(interServerRequestHandlerRegister);
registerServer.listen(portRegisterServer);
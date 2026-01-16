import http          from 'node:http';
import {join}        from 'node:path';
import {readFile}    from 'node:fs';
import {Server}      from 'socket.io';
import {MongoClient} from 'mongodb';


const httpServer = http

    .createServer((request, response) => {
        let {url} = request;
        if(url == '/'){
            url = '/cliente.html';
            const filename = join(process.cwd(), url);

            readFile(filename, (err, data) => {
                if(!err){
                    response.writeHead(200, {'Content-Type': 'text/html; charset=utf-8'});
                    response.write(data);
                    response.end();
                }
                else{
                    response.writeHead(200, {"Content-Type": "text/plain"});
					response.write('Error de lectura en el fichero: '+ url);
					response.end();
                }
            });
        }
        else{
            const filename = join(process.cwd(), url + '.html');

            readFile(filename, (err, data) => {
                if(!err){
                    response.writeHead(200, {'Content-Type': 'text/html; charset=utf-8'});
                    response.write(data);
                    response.end();
                }
                else{
                    response.writeHead(200, {"Content-Type": "text/plain"});
					response.write('Error de lectura en el fichero: '+ url);
					response.end();
                }
            });
        }
    });

    var estado_persiana = 'cerrada';
    var estado_ac = 'apagado';
    var estado_radiador = 'apagado';
    var estado_luces = 'apagadas';
    var estado_humidificador = 'apagado';
    var estado_deshumidificador = 'apagado';

	var allClients = new Array();
    var alertas = [];

    MongoClient.connect("mongodb://localhost:27017/").then((db) => {
        const dbo = db.db("pruebaBaseDatos");
		const sensores = dbo.collection("sensores");

        const io = new Server(httpServer);

        io.sockets.on('connection', (client) => {
            const cAddress = client.request.socket.remoteAddress;
            const cPort = client.request.socket.remotePort;

            allClients.push({address:cAddress, port:cPort});
            console.log(`Nueva conexión de ${cAddress}:${cPort}`);

            io.sockets.emit('all-connections', allClients);

            client.on('actualizar-sensores', (data) =>{
                console.log('Actualizando sensores');
                sensores.insertOne(data, {safe:true}).then((result) => {
                    io.sockets.emit('actualizar-sensores',{
                        temperatura: data.temperatura,
                        luminosidad: data.luminosidad,
                        humedad: data.humedad,
                        time:data.time
                    });
                    console.log(data);
                });

            });

            client.on('obtener', () => {
                sensores.find().sort({_id:-1}).limit(1).forEach(function(result){
                    client.emit('obtener',{
                        temperatura: result.temperatura,
                        luminosidad: result.luminosidad,
                        humedad: result.humedad,
                        time:result.time
                    });
                });
            })

            client.on('historico', () => {
                sensores.find().toArray().then((results) => {
                    client.emit('historico', results);
                });
            });

            client.on('alerta', (data) =>{
                alertas.push(data);
                io.sockets.emit('alerta', data);
            });

            client.on('mostrar-alertas',()=>{
                io.sockets.emit('mostrar-alertas', alertas);
            });

            client.on('borrar-alertas', () =>{
                alertas = [];
                io.sockets.emit('mostrar-alertas', alertas);
            });


            // ACTUADORES
            client.on('obtener_estado_persiana', () =>{
                client.emit('persiana', estado_persiana);
            });

            client.on('persiana', () =>{
                if(estado_persiana == 'abierta')
                    estado_persiana = 'cerrada';
                else
                    estado_persiana = 'abierta';

                io.sockets.emit('persiana', estado_persiana);
                console.log("La persiana está: " + estado_persiana);
            });

            client.on('obtener_estado_aire', () =>{
                client.emit('aire', estado_ac);
            });

            client.on('aire', () =>{
                if(estado_ac == 'encendido')
                    estado_ac = 'apagado';
                else
                    estado_ac = 'encendido';

                io.sockets.emit('aire', estado_ac);
                console.log("El aire está: " + estado_ac);
            });

            client.on('obtener_estado_radiador', () =>{
                client.emit('radiador', estado_radiador);
            });

            client.on('radiador', () =>{
                if(estado_radiador == 'encendido')
                    estado_radiador = 'apagado';
                else
                    estado_radiador = 'encendido';

                io.sockets.emit('radiador', estado_radiador);
                console.log("El radiador está: " + estado_radiador);
            });

            client.on('obtener_estado_humidificador', () =>{
                client.emit('humidificador', estado_humidificador);
            });

            client.on('humidificador', () =>{
                if(estado_humidificador == 'encendido')
                    estado_humidificador = 'apagado';
                else
                    estado_humidificador = 'encendido';

                io.sockets.emit('humidificador', estado_humidificador);
                console.log("El humidificador está: " + estado_humidificador);
            });

            client.on('obtener_estado_deshumidificador', () =>{
                client.emit('deshumidificador', estado_deshumidificador);
            });

            client.on('deshumidificador', () =>{
                if(estado_deshumidificador == 'encendido')
                    estado_deshumidificador = 'apagado';
                else
                estado_deshumidificador = 'encendido';

                io.sockets.emit('deshumidificador', estado_deshumidificador);
                console.log("El deshumidificador está: " + estado_deshumidificador);
            });

            client.on('obtener_estado_luces', () =>{
                client.emit('luces', estado_luces);
            });

            client.on('luces', () =>{
                if(estado_luces == 'encendidas')
                    estado_luces = 'apagadas';
                else
                    estado_luces = 'encendidas';

                io.sockets.emit('luces', estado_luces);
                console.log("Las luces están: " + estado_luces);
            });

            

            client.on('disconnect', () => {
                console.log(`El usuario ${cAddress}:${cPort} se va a desconectar`);

                const index = allClients.findIndex(cli => cli.address == cAddress && cli.port == cPort);

                if (index != -1) {
                    allClients.splice(index, 1);
                    io.sockets.emit('all-connections', allClients);
                } else {
                    console.log('¡No se ha encontrado al usuario!')
                }
                console.log(`El usuario ${cAddress}:${cPort} se ha desconectado`);
            });


            client.on('resetear', () => {
                console.log('Se ha reseteado la base de datos');
                dbo.collection("sensores").drop();
                io.sockets.emit('resetear', () => {});
            });

        });

        httpServer.listen(8080);
    }).catch((err) => {console.error(err)});

console.log('Servicio Socket.io iniciado');

const express = require("express");
const app = express();
const http = require("http");
const server = http.createServer(app);
const { Server, Socket } = require("socket.io");
const io = new Server(server);

const moment = require('moment');

const mysql = require("mysql");
const { use } = require("express/lib/router");


// PERSIAPAN MENGHUBUNGKAN DATABASE
const connection = mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: 'ketutpandoyo.123',
    database: 'chat_widget'
});

app.use(express.static('.'));

// PATH UNTUK USERS
app.get('/users', (req, res) => {
    res.sendFile(__dirname + '/index.php');
});

// PATH UNTUK LOGIN ADMIN
app.get('/login_admin', (req, res) => {
    res.sendFile(__dirname + '/views/login_admin.html');
});

// PATH UNTUK DASHBOARD ADMIN
app.get('/admin', (req, res) => {   
    res.sendFile(__dirname + '/views/admin.html');
});

var rooms = [];
var usernames = [];

// MELAKUKAN KONEKSI SOCKET
io.on('connection', (socket) => {

    // MENGHUBUNGKAN USER
    socket.on('create_user', (username, email) => {
        connection.query(
            'INSERT INTO customers(username, email) SELECT ?, ? FROM (select 1)a where not exists (select id from customers where email=?)',
            [username, email, email],
            (error, results) => {
                if (results.affectedRows == 1) {
                    // JIKA USER SUDAH TERDAFTAR
                    const getId = results.insertId;

                    // MASUK KE ROOM BERDASARKAN ID
                    socket.join(getId);
                    socket.room = getId;

                    rooms.push(getId);
                    usernames.push(username);

                    io.emit('updaterooms', rooms, usernames);
                } else {
                    // JIKA USER BELUM TERDAFTAR
                    connection.query(
                        'SELECT id FROM customers WHERE email=?',
                        [email],
                        (error, results) => {
                            const getId = results[0].id;

                            if (!(rooms.includes(getId))) {
                                rooms.push(getId);
                                usernames.push(username);
                            }

                            // MASUK KE ROOM BERDASARKAN ID
                            socket.join(getId);
                            socket.room = getId;

                            historyChat(getId);

                            io.emit('updaterooms', rooms, usernames);
                        }
                    )
                }
            }
        )
    });

    // LOGIN UNTUK ADMIN
    socket.on('login_admin', (username, password) => {
        connection.query(
            "SELECT id FROM admins WHERE username = ? AND password = ?",
            [username, password],
            (error, results) => {
                socket.emit('login_result', results.length);
            }
        )
    });

    // KETIKA ADMIN MASUK KE DASHBOARD
    socket.on('create_admin', (username) => {
        if (!(rooms.length >= 1)) {
            connection.query(
                'SELECT id, username FROM customers',
                (error, results) => {
                    results.forEach((item) => {
                        rooms.push(item.id);
                        usernames.push(item.username);
                    })
                }
            )
        }

        io.emit('updaterooms', rooms, usernames);
    });

    // MENGIRIM CHAT UNTUK USER DAN ADMIN
    socket.on('sendchat', (username, msg) => {
        io.of('/').adapter.rooms.forEach((value, key) => {
            if (key == socket.room) {
                io.in(key).emit('updatechat', username, msg);
            }
        });

        const date = moment().format('YYYY-MM-DDThh:mm:ss.ms');
        connection.query(
            'INSERT INTO `history` (`roomid`, `person`, `msg`, `date`) VALUES (?, ?, ?, ?)',
            [socket.room, username, msg, date],
            (error, results) => {
                console.log("Insert to table history success...");
            }
        )
    });

    // MENGGANTI ROOM UNTUK ADMIN
    socket.on('switchRoom', function (newroom) {
        var oldroom;
        oldroom = socket.room;
        socket.leave(socket.room);
        socket.join(newroom);
        socket.room = newroom;

        historyChat(socket.room);
    });

    // socket.on('delete_chat', (room) => {
    //     console.log(room);
    //     connection.query(
    //         'DELETE FROM history WHERE roomid = ?',
    //         [room],
    //         (error, results) => {
    //             console.log("Delete chat success...");
    //         }
    //     )
    // })

    // MENAMPILKAN HISTORY CHAT BERDASARKAN ROOM
    function historyChat(roomId) {
        connection.query(
            'SELECT * FROM history WHERE roomid = ?',
            [roomId],
            (error, results) => {
                results.forEach((item) => {
                    socket.emit('updatechat', item.person, item.msg);
                });
            }
        )
    }
});

server.listen(8080, () => {
    console.log("listening port: " + "http://localhost:8080/");
});
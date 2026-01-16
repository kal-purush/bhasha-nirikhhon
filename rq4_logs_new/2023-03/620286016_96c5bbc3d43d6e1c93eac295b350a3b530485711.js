const express = require("express");
const http = require("http");
const cors = require("cors");
const nodemailer = require("nodemailer");
const app = express();
const { Server } = require("socket.io");

app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const PORT = 7000;

server.listen(PORT, () => console.log(`Server is running on port ${PORT}`));

app.post("/invite-user", async (req, res) => {
  console.log(req.body, "invite user");

  try {
    await sendEmailInvitation(req.body);

    res.send({ status: "SUCCESS" });
  } catch (error) {
    console.log(error);
    res.send({ status: "FAILED" });
  }
});

function sendEmailInvitation(recipient) {
  return new Promise((res, rej) => {
    const transporter = nodemailer.createTransport({
      service: "gmail",
      auth: {
        user: "empty.anec@gmail.com",
        pass: "texlbtfnnumcsyvn",
      },
    });

    const mailOptions = {
      from: "Empty <empty.anec@gmail.com>",
      to: recipient.to,
      subject: "Invite to the account!",
      html: `<p>User ${recipient.from} invite your to the account: '${recipient.accountName}'! You can find an account by id: '<b>${recipient.accountId}</b>'</p>
        <a href="http://localhost:3000" target="_blank">Link</a>`,
    };

    transporter.sendMail(mailOptions, (err) => {
      err ? rej(false) : res(true);
    });
  });
}

const io = new Server(server, {
  cors: {
    origin: ["http://localhost:3000"],
    credentials: true,
  },
});

let roomId;

io.on("connection", (socket) => {
  console.log(`User connected: ${socket.id}`);

  socket.on("join_account", (accountId) => {
    console.log(`Account ID ${accountId}`);
    roomId = accountId;
    socket.join(roomId);

    socketEmitToRoom(socket, "joined_to_account_success");
  });

  socket.on("boards_update", () => {
    socketEmitToRoom(socket, "boards_updated_trigger");
  });

  socket.on("cards_update", () => {
    socketEmitToRoom(socket, "cards_updated_trigger");
  });
});

function socketEmitToRoom(socket, event, payload = {}) {
  socket.to(roomId).emit(event, payload);
}
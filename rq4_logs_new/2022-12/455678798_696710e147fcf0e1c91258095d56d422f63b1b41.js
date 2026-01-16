const express = require("express");
const app = express();
const bodyParser = require("body-parser")

const mongoose = require("mongoose");

mongoose.connect("mongodb+srv://admin-m4hosam:m4h123456@cluster0.fr5st.mongodb.net/commentsData", { useNewUrlParser: true, useUnifiedTopology: true })
    .then(() => {
        console.log('Conneccted To DataBase')
    })
    .catch(er => {
        console.log('Connection Error To DataBase')
    });

// next line to enable views directory
app.set('view engine', 'ejs');
// important to make bodyParser work effieciently
app.use(bodyParser.urlencoded({ extended: true }));
// access any other files withen ejs(css js images) to public directory
app.use(express.static("public"))

let currentuser = -1;
const users = ["amyrobson", "juliusomo", "maxblagun", "ramsesmiron"]


const replySchema = new mongoose.Schema({
    id: Number,
    content: String,
    createdAt: String,
    score: Number,
    replyingTo: String,
    username: String,
    repliedToId: String
})

const commentSchema = new mongoose.Schema({
    id: Number,
    content: String,
    createdAt: String,
    score: Number,
    username: String,
    replies: [{
        type: mongoose.Schema.Types.ObjectId,
        ref: 'Reply'
    }]
})

const Comment = mongoose.model("Comment", commentSchema);
const Reply = mongoose.model("Reply", replySchema);

function CommentOb(id, content, createdAt, score, username, replies) {
    this.id = id;
    this.content = content;
    this.createdAt = createdAt;
    this.score = score;
    this.username = username;
    this.replies = replies;
}
function RepliesOb(id, content, createdAt, score, username, replyingTo, repliedToId) {
    this.id = id;
    this.content = content;
    this.createdAt = createdAt;
    this.score = score;
    this.username = username;
    this.replyingTo = replyingTo;
    this.repliedToId = repliedToId;
}


function elapsedTime(date) {
    start = new Date(date) // in the data base
    const end = new Date();
    const elapsed = Math.floor((end - start) / 1000);
    if (elapsed == 0)
        return "1 second ago"
    const years = Math.floor(elapsed / (3600 * 24 * 30 * 12));
    const months = Math.floor(elapsed % (3600 * 24 * 30 * 12) / (3600 * 24 * 30));
    const days = Math.floor(elapsed % (3600 * 24 * 30) / (3600 * 24));
    const hours = Math.floor(elapsed % (3600 * 24) / 3600);
    const mins = Math.floor(elapsed % 3600 / 60);
    const seconds = Math.floor(elapsed % 60);
    const timeString = ["years", "months", "days", "hours", "mins", "seconds"];
    const time = [years, months, days, hours, mins, seconds];
    let i = 0;
    while (i < 6 && time[i] == 0) {
        i++;
    }
    return time[i] + " " + timeString[i] + " ago";
}
// let start = "Tue Feb 15 2022 17:35:51 GMT+0300 (GMT+03:00)"

function updateScore(type, comentData) {
    if (type == "add")
        comentData.score = comentData.score + 1;
    else if (type == "minus") {
        if (comentData.score > 0)
            comentData.score = comentData.score - 1;
    }
    else {
        console.log("Error");
    }
    return comentData;
}


app.get("/", function (req, res) {
    res.render("home");
})

app.post("/", function (req, res) {
    currentuser = req.body.id;
    res.redirect('/comments');
})

app.get("/comments", function (req, res) {
    // populate used to excute the relateed database in replies
    if (currentuser == -1)
        res.redirect('/');
    else {
        Comment.find({}).populate("replies").exec(async function (err, docs) {
            if (err)
                console.log(err)
            else {
                for (let element of docs) {
                    element.createdAt = elapsedTime(element.createdAt);
                    if (element.replies.length > 0) {
                        for (let replyElement of element.replies) {
                            replyElement.createdAt = elapsedTime(replyElement.createdAt);
                        }
                    }
                }
                res.render("comments", { currentId: currentuser, currentUsername: users[currentuser - 1], data: docs });
            }
        })
    }
})
app.post("/newComment", async function (req, res) {
    let newId = users.indexOf(req.body.username) + 1;
    let obj = new CommentOb(newId, req.body.content, (new Date()).toString(), 0, req.body.username, []);
    let d = new Comment(obj);
    await d.save();
    res.redirect('/comments');
})

// id, content, createdAt, score, username, replyingTo, repliedToId
app.post("/reply", async function (req, res) {
    let newId = users.indexOf(req.body.username) + 1;
    let obj = new RepliesOb(newId, req.body.content, (new Date()).toString(), 0, req.body.username, req.body.repliedAt, req.body.commentId);
    // console.log(obj)
    Comment.findById(req.body.commentId, async function (err, docs) {
        if (err)
            console.log(err);
        else {
            const newReply = new Reply(obj)
            await newReply.save();
            docs.replies.push(newReply);
            await docs.save();
            res.redirect('/comments');
        }
    })
})



app.post("/score", async function (req, res) {
    // Error
    if (req.body.commentId == undefined) {
        Reply.findById(req.body.replyId, async function (er, replyData) {
            if (er)
                console.log(er);
            else if (replyData != null) {
                replyData = updateScore(req.body.op, replyData);
                await replyData.save();
                res.redirect('/comments');
            }
        })
    }
    else {
        Comment.findById(req.body.commentId, async function (err, commentData) {
            if (err)
                console.log(err);
            else if (commentData != null) {
                commentData = updateScore(req.body.op, commentData);
                await commentData.save();
                res.redirect('/comments');
            }
        })
    }

})

app.post("/delete", async function (req, res) {
    if (req.body.commentId == undefined) {
        Reply.deleteOne({ _id: req.body.replyId }, async function (er) {
            if (er)
                console.log(er);
        })
    }
    else {
        Comment.findOneAndDelete({ _id: req.body.commentId }, async function (err, commentData) {
            if (err)
                console.log(err);
            else {
                for (const element of commentData.replies) {
                    Reply.deleteOne({ _id: element._id }, async function (e) {
                        if (e)
                            console.log(e);
                    })
                }
            }
        })
    }
    res.redirect('/comments');
})



app.post("/edit", async function (req, res) {
    if (req.body.idType == "commentId") {
        Comment.findById(req.body.id, async function (err, commentData) {
            if (err)
                console.log(err);
            else if (commentData != null) {
                commentData.content = req.body.content;
                commentData.createdAt = (new Date()).toString();
                await commentData.save();
                res.redirect('/comments');
            }
        })
    }
    else if (req.body.idType == "replyId") {
        Reply.findById(req.body.id, async function (err, commentData) {
            if (err)
                console.log(err);
            else if (commentData != null) {
                commentData.content = req.body.content;
                commentData.createdAt = (new Date()).toString();
                await commentData.save();
                res.redirect('/comments');
            }
        })
    }
})


app.listen(process.env.PORT || 3000, function () {
    console.log("Connected To The Server");
})
const express = require("express").Router();
const route = require("express").Router();

const Article = require('./module');

// Request Targeting specific articles
// app.route("/articles/:articleTitle")
route.get("/showone",async (req, res) => {
    const articleRequested = req.params.articleTitle;
    try {
        const foundArticle = await Article.findOne({ title: articleRequested });
        if (!foundArticle) {
            res.status(404).send("Article not found");
        } else {
            res.send(foundArticle);
        }
    } catch (err) {
        console.error(err);
        res.status(500).send("Error fetching article");
    }
})


// update using put
route.put("update", async (req, res) => {
    const articleRequested = req.params.articleTitle;
    const { title, content } = req.body;
    try {
        await Article.updateOne(
            { title: articleRequested },
            { title, content },
            { overwrite: true }
        );
        res.send("Successfully updated article");
    } catch (err) {
        console.error(err);
        res.status(500).send("Error updating article");
    }
})

// update using patch

route.patch("UpdateOne", async (req, res) => {
    const articleRequested = req.params.articleTitle;
    const { title, content } = req.body;
    try {
        await Article.updateOne(
            { title: articleRequested },
            { $set: { title, content } }
        );
        res.send("Successfully updated article");
    } catch (err) {
        console.error(err);
        res.status(500).send("Error updating article");
    }
})

    route.delete("/deleteOne", async (req, res) => {
        const articleRequested = req.params.articleTitle;
        try {
            await Article.deleteOne({ title: articleRequested });
            res.send("Successfully deleted article");
        } catch (err) {
            console.error(err);
            res.status(500).send("Error deleting article");
        }
    });

// Request Targeting all articles
// app.route("/articles")
route.get("/articles", async (req, res) => {
    try {
        const foundArticles = await Article.find();
        res.send(foundArticles);
    } catch (err) {
        console.error(err);
        res.status(500).send("Error fetching articles");
    }
})

    route.post("/Newdata",async (req, res) => {
        const { title, content } = req.body;
        try {
            const newArticle = new Article({ title, content });
            await newArticle.save();
            res.send("Successfully saved new article");
        } catch (err) {
            console.error(err);
            res.status(500).send("Error saving article");
        }
    })

    route.delete("deleteAll", async (req, res) => {
        try {
            await Article.deleteMany();
            res.send("Successfully deleted all articles");
        } catch (err) {
            console.error(err);
            res.status(500).send("Error deleting articles");
        }
    });

module.exports = route;
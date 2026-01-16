module.exports = {
    routes: [
        {
            method: "GET",
            path: "/sentiment",
            handler: "newsstock.getSentiment",
        },
        {
            method: "GET",
            path: "/newsfeed",
            handler: "newsstock.getNews",
        }
    ],
};
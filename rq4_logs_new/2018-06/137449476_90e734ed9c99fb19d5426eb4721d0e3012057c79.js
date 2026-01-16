const fs = require('fs');

class ServFunc {
    responseJson(data, response) {
        response.writeHead(200, {"Content-Type": "application/json"});
        let json = JSON.stringify(data);
        response.end(json);      
    }
    
    responseFunc(response) {
        response.writeHead(200);
        response.end();   
    }

    respErrFunc(text, response)  {
        response.writeHead(404, {'Content-Type': 'text/plain'});
        response.end(text + " not found!");
    }
    
    sendFile(user, response) {                
        let json = JSON.stringify({Articles:user.articles});
        let time = new Date(Date.now());
        let filePath = `user_${user.ID}_${time.getHours()}h_${time.getMinutes()}min.json`;
        fs.writeFile(filePath, json, (err) => {
            if (err) throw err;
        });
        response.writeHead(200, {
            'Content-Disposition': `attachment; filename=${filePath}`
        });
        fs.createReadStream(filePath).pipe(response);
    }
    getUserAndNews(pathname, withNews, users, topics) {
        if (withNews == true) {
            let newsID = parseInt(pathname.split("/")[2]);
            let userID = parseInt(pathname.split("/")[4]);
            let news = topics.filter(storTopic => newsID === storTopic.ID);
            let user = users.filter(storUser => userID === storUser.ID);
            return [news[0], user[0]];
        } else {
            let userID = parseInt(pathname.split("/")[2]);
            let user = users.filter(storUser => userID === storUser.ID);
            return user[0];
        }   
    }
}

 module.exports = ServFunc;
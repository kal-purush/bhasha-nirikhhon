const db = require('./dbConnection');

function updateVolState(req, res) {

let query='';
if(req.body.key[0]=='States')
    {
        query = 'with s(state_ID) as(\
            SELECT state_ID FROM '+req.body.key[0]+' Where abbr = ?\
            ) update Volunteers set state = (select s.state_ID from s) where vol_ID=?; ';
       }
else{query=query = 'with s(shul_ID) as(\
    SELECT shul_ID FROM '+req.body.key[0]+' Where name = ?\
    ) update Volunteers set state = (select s.shul_ID from s) where vol_ID=?; ';
}    

    db.query(query, [req.body.value, req.body.id],
         (error, response) => {
        console.log(error || response);

        if (response && response.length > 0) {
            res.status(200);
            res.json(response);
        }
        else if (response) {
            res.sendStatus(204);
        }
        else {
            res.sendStatus(500);
        }
    });


}
module.exports = updateVolState
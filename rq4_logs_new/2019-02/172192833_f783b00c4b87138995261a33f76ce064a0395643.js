module.exports={
    getAll: ( req, res ) => {
        const db = req.app.get('db')

        db.get_all_houses().then(resp => {
            res.status(200).send(resp);
        })
    }
}
const { age, date } = require("../../lib/utilities");

const db = require("../../config/db")



module.exports = {
    //seleciona todos os instrutores
    all(callback) {

        db.query(`SELECT * 
        FROM members
        ORDER BY name ASC`, function (err,results) {//`SELECT * FROM members`, pegando todos os campos daquela tabela no banco de dados
            if (err) throw "Database Error"
            
            callback(results.rows)// enviando os dados como parametro
        })

    },
    //criando novo (post)
    create(data, callback) {


        //fazendo a query usando template string com a função de inserir dados, tabela já criada
        const query = `
            INSERT INTO members (
                name,
                avatar_url,
                email,
                ocupation,
                origin,
                blood,
                height,
                weight,
                captain_id
            )   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING id
        `; //assim que ele rodar e cadastrar no banco ele irá me retornar um id

        //criando array que será utilizado para substituir o splaceholders, "$1, $2, $3, $4, $5, $6"
        const values = [
            data.name,
            data.avatar_url,
            data.email,
            data.ocupation,
            data.origin,
            data.blood,
            data.height,
            data.weight,
            data.captain // somente captain pois no njk já esta chamando captain.id
        ];
                            //callback function
        db.query(query, values, function(err,results) {
            if (err) throw `Database error! ${err}`

            callback(results.rows[0])
        })

    },
    find(id, callback) {
        //'filtrando'  //"PEGA TUDO DE MEMBERS* E DO CAPTAIN EU QUERO SÓ O NAME, sendo captain_name"
        db.query(`SELECT members.*, captains.name AS captain_name
                FROM members 
                LEFT JOIN captains ON (members.captain_id = captains.id)
                WHERE members.id = $1`, [id], function(err, results) {
                    
                    if (err) throw `Database Error!${err}`

                    
                    callback(results.rows[0])
        })
    },
    update(data,callback){
        const query = `
        UPDATE members SET 
            avatar_url=($1),
            name=($2),
            email=($3),
            ocupation=($4),
            origin=($5),
            blood=($6),
            height=($7),
            weight=($8),
            captain_id=($9)
        WHERE id = $10     
    `//NUNCA ESQUECA QUE SEU UPDATE PRECISA TER UM WHERE


        const values = [
            data.avatar_url,
            data.name,
            data.email,
            data.ocupation,
            data.origin,
            data.blood,
            data.height,
            data.weight,
            data.captain,
            data.id
        ]

        db.query(query, values, function(err, results){
            if (err) throw `Database error! ${err}`

            callback()// Não precisanddo devolver nada pois no members ele já enviará os dados do req.body
        })

    },
    delete(id, callback){
        db.query(`DELETE FROM members WHERE id = $1`, [id], function(err, results){
            if (err) throw `Database error! ${err}`

            //se tudo deu certo...
            return callback()
        })
    },
    captainsSelectOptions(callback){
        db.query(`SELECT name, id FROM captains`, function(err, results){
            if (err) throw 'Database Error!'

            callback(results.rows)
        })
    }
}
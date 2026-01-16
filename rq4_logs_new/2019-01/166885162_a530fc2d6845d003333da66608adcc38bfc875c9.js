const express = require('express');
const knex = require('knex');
const knexConfig = require('../knexfile');

const protect = require('../protected/protected');

const router = express();
const db = knex(knexConfig.development);

router.use(express.json());
router.use(protect.protect);

//All Routes

router.get('/ids', (req, res)=>{
    db('users')
    .select('id')
    .then(users=>{
        res.status(200).json(users);
    })
    .catch(error=>{
        res.status(500).json({error: 'Failed to return users'})
    })
})

router.get('/usernames', (req, res)=>{
    db('users')
    .select('username')
    .then(users=>{
        res.status(200).json(users);
    })
    .catch(error=>{
        res.status(500).json({error: 'Failed to return users'})
    })
})

module.exports = router;
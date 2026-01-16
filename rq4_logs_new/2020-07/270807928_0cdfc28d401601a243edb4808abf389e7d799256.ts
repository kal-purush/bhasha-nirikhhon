import { PoolClient } from "pg";
import { connectionPool, schema } from ".";






// save a new browse
export async function saveBrowsingHistory( bookId:number, userId:number){
    let client:PoolClient
    try{
        client = await connectionPool.connect()
        await client.query('BEGIN;')
        let select = await client.query(`select * from ${schema}.browsing_history bh 
                                        where bh."book_id" = $1 and bh."user_id" = $2;`, [bookId,userId])
        if(select.rowCount){
            let res = await client.query(`update ${schema}.browsing_history set "time_viewed" = now() 
            where "book_id" = $1 and "user_id" = $2;`, [bookId,userId])
            console.log(res);
            
        }else {
            await client.query(`insert into ${schema}.browsing_history ("book_id", "user_id")
            values ($1,$2);`, [bookId, userId])
        }
        await client.query('COMMIT;')                          
        return true
    }catch(e){
        client.query('ROLLBACK;')
        console.log(e);
        throw new Error('Unhandled Error Occurred')
    }finally{
        client?.release()
    }
}

//fetch all broswing related to a user
export async function getBrowserHistoryByUserId(userId:number):Promise<number[]>{
    let client:PoolClient
    try{
        client = await connectionPool.connect()
        let res = await client.query(`select bh.book_id from ${schema}.browsing_history bh 
                                        where bh.user_id = $1 order by bh.time_viewed desc;`, [userId])
        return res.rows.map((ele)=>{
            return ele.book_id
        })
    }catch(e){
        console.log(e)
        throw new Error('Unhandled Error Occurred')
    }finally{
        client?.release()
    }
}
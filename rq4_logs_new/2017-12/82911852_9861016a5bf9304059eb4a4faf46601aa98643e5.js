let SMysql = require('../index.js')

let db = {  
  "host"     : "ip1",  
  "user"     : "root",  
  "password" : "pwd2",  
  "port"     : "3306"
}
let db2 = {  
  "host"     : "ip2",  
  "user"     : "root",  
  "password" : "pwd2",  
  "port"     : "3306"
}

let data = [
    {'name': 'test1','num': 1},
    {'name': 'test2','num': 2},
    {'name': 'test3','num': 3}
]
/** 
    step1 : 新建数据库
*/
function createSql(name) {
    return new Promise((resolve,reject)=>{
        let sMysql = new SMysql(db);

        sMysql.createSql(name).end((data)=>{
            console.log(data[0]);
            resolve(name);
        });
    })
}

/** 
    step2 : 
        新建数据表
        新增数据
        查询数据（返回name字段）
        更新数据
        查询数据（返回所有数据）
        删除数据（{'name': 'test1','num': 1}）
        查询数据（返回所有数据）
        复制数据表到newTableName
        查询数据（返回newTableName表所有数据）
        删除数据表（tableName和newTableName）
*/

function controlTable(sqlName, tableName, newTableName) {
    return new Promise((resolve,reject)=>{
        let sMysql = new SMysql(db,sqlName);

        sMysql
            .createTable(tableName,{
                'name' : {
                    type: 'VARCHAR',
                    length: 50,
                    isNull: false
                },
                'num' : {
                    type: 'INT',
                    length: 20,
                    isNull: false
                }
            })
            .insert(tableName,data)
            .search(tableName,'name')
            .update(tableName,{'name':'test4'},{'name': 'test3','num': '3'})
            .search(tableName)
            .deleteData(tableName, {
                'name': 'test1',
                'num': '1'
            })
            .search(tableName)
            .copyTable(tableName,newTableName)
            .search(newTableName)
            .deleteTable([tableName,newTableName])
            .end((data)=>{
                for(let value of data) {
                    console.log(value);
                }
                resolve();
            });
    })
}

/** 
    step3 : 删除数据库
*/

function deleteSql(name) {
    return new Promise((resolve,reject)=>{
        let sMysql = new SMysql(db);

        sMysql.deleteSql(name).end((data)=>{
            console.log(data[0]);
            resolve();
        });
    })
}

async function play() {
    let sql = await createSql('test');
    await controlTable(sql,'table1','table2');
    await deleteSql(sql);
}

/** 
    复制数据表 : 复制服务器db，数据库blog，数据表article 到服务器db2
*/

function copyTable() {
    SMysql.copyTable(db, db2, 'blog', 'article', ()=>{
        console.log()
    });
}
play();
// copyTable();
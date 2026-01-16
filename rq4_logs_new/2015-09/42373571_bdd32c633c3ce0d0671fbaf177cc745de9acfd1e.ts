
/// <reference path="../typings/lovefield/lovefield.d.ts" />
import 'lovefield/dist/lovefield';

function sequence( ...items: any[][] ) {
    var d = items.pop();
    if (d){
        var fn : (...args:any[]) => Promise<any> = d.splice(0,1)[0];
        var args = d;
        fn(args).then(()=>{ sequence(...items)})
    }
}
    
(function(){ 
    var schemaBuilder: lf.schema.Builder = lf.schema.create('todo', 1);
    
    schemaBuilder.createTable('Item')
    .addColumn('id', lf.Type.INTEGER)
    .addColumn('description', lf.Type.STRING)
    .addColumn('deadline', lf.Type.DATE_TIME)
    .addColumn('done', lf.Type.BOOLEAN)    
    .addPrimaryKey(['id'])  /*.addPrimaryKey(['id'],true)  AHHH dont use autoinc key option!!! no wonder they removed this from the typedef. performance is soooo bad! */
    .addIndex('idxDeadline', ['deadline'], false, lf.Order.DESC);
    
    
    var connectOptions: lf.schema.ConnectOptions;
    var todoDb: lf.Database = null;
    var dummyItem: lf.schema.Table = null;
    var inserts:number = 50000;
    
    sequence(
        [testSelectPredicate,'INDEXED_DB'],
        [testSelectPredicate, 'WEB_SQL'],
        [testSelect,'INDEXED_DB'],
        [testSelect, 'WEB_SQL'],
        [testInsert,'INDEXED_DB'],
        [testInsert, 'WEB_SQL'],
        [dropTable, 'INDEXED_DB'],
        [dropTable, 'WEB_SQL'],
        [testInsertBatch,'INDEXED_DB'],
        [testInsertBatch,'WEB_SQL'],
        [dropTable, 'INDEXED_DB'],
        [dropTable, 'WEB_SQL']    
    );  
    
    function dropTable(storeType:string){
        connectOptions = {
            storeType: lf.schema.DataStoreType[storeType]                        
        }; 
        var starttime:number = Date.now();           
        return schemaBuilder.connect(connectOptions).then(db =>{
            dummyItem = db.getSchema().table('Item');
            return db.delete().from(dummyItem).exec();
        })
        .then(()=>{
            var duration = Date.now() - starttime;
            console.log(`lovefield:${storeType} deleting ${inserts} rows ${duration}ms`);
        })   
    }
    
    function testInsert(storeType: string) {        
        return new Promise((resolve,reject)=>{
            connectOptions = {
                storeType: lf.schema.DataStoreType[storeType]
            };
            var starttime:number = Date.now();
            
            schemaBuilder.connect(connectOptions).then(db => {
                todoDb = db;                
                dummyItem = db.getSchema().table('Item');
                
                var tx = db.createTransaction();
                var q = [];
                
                for (var i=0; i<inserts; i++) {
                var row = dummyItem.createRow({
                    'id': i,
                    'description': 'Get a cup of coffee',
                    'deadline': new Date(),
                    'done': false
                });
                
                q.push(db.insert().into(dummyItem).values([row]));
                }
                return tx.exec(q);
            }).then(()=>{
                var duration = Date.now() - starttime;
                console.log(`lovefield:${storeType} ${inserts} inserts ${duration}ms`);
                resolve();
            });
        });
    }
    
    function testInsertBatch(storeType: string) {        
        return new Promise((resolve,reject)=>{
            connectOptions = {
                storeType: lf.schema.DataStoreType[storeType]
            };
            var starttime:number = Date.now();
            
            schemaBuilder.connect(connectOptions).then(db => {
                todoDb = db;                
                dummyItem = db.getSchema().table('Item');
                
                var rows = [];                
                for (var i=0; i<inserts; i++) {
                    var row = dummyItem.createRow({
                        'id': i,
                        'description': 'Get a cup of coffee',
                        'deadline': new Date(),
                        'done': false
                    });
                    rows.push(row);
                }
                return db.insert().into(dummyItem).values(rows).exec();
            }).then(()=>{
                var duration = Date.now() - starttime;
                console.log(`lovefield:${storeType} 1 insert ${inserts} rows ${duration}ms`);
                resolve();
            });
        });
    }    
    
    
    function testSelectPredicate(storeType: string) {     
        return new Promise((resolve,reject)=>{
            connectOptions = {
                storeType: lf.schema.DataStoreType[storeType]
            };
            var starttime:number = Date.now();
            
            schemaBuilder.connect(connectOptions).then(db => {
                    todoDb = db;                
                    dummyItem = db.getSchema().table('Item');
                    
                    var tx = db.createTransaction();
                    var q = [];
                    
                    for (var i=0; i<inserts; i++) {            
                    var column: lf.schema.Column = (<any>dummyItem).id;
                    var select = todoDb.select().from(dummyItem).where(column.eq(10000));            
                    q.push(select);
                    }
                    return tx.exec(q);
            }).then(()=>{
                var duration = Date.now() - starttime;
                console.log(`lovefield:${storeType} ${inserts} selects single row ${duration}ms`);
                resolve();
            });
        });
    }  

    function testSelect(storeType: string) {     
        return new Promise((resolve,reject)=>{
            connectOptions = {
                storeType: lf.schema.DataStoreType[storeType]
            };
            var starttime:number = Date.now();
            
            schemaBuilder.connect(connectOptions).then(db => {
                    todoDb = db;                
                    dummyItem = db.getSchema().table('Item');                                          
                    var select = todoDb.select().from(dummyItem);            
                    
                    return select.exec();
            }).then((rows)=>{
                if (!rows.length || rows.length < inserts) console.error( `lovefield:${storeType} did not select the expected number of rows!`);
                else {
                    var duration = Date.now() - starttime;
                    console.log(`lovefield:${storeType} 1 select ${inserts} rows ${duration}ms`);
                }
                resolve();
            });
        });
    }  
   
    
})();
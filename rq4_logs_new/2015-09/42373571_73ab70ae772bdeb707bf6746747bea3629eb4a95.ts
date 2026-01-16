/// <reference path="../typings/tsd.d.ts" />

///// <reference path="../typings/lovefield/lovefield.d.ts" />
import 'lovefield/dist/lovefield';

function sequence( ...items: any[][] ) {
    var d = items.pop();
    if (d){
        var fn : (...args:any[]) => Promise<any> = d.splice(0,1)[0];
        var args = d;        
        fn.apply(this,args).then(()=>{ sequence(...items)})
    }
}
    
(function(){ 
    var schemaBuilder: lf.schema.Builder = lf.schema.create('todo', 1);
    
    schemaBuilder.createTable('Item')
    .addColumn('id', lf.Type.INTEGER)
    .addColumn('description', lf.Type.STRING)
    .addColumn('deadline', lf.Type.DATE_TIME)
    .addColumn('done', lf.Type.BOOLEAN)    
    /*.addPrimaryKey(['id'],true)  AHHH dont use autoinc key option!!! no wonder they removed this from the typedef. performance is soooo bad! */
    .addIndex('idxDeadline', ['deadline'], false, lf.Order.DESC);
    
    
    var connectOptions: lf.schema.ConnectOptions;
    var todoDb: lf.Database = null;
    var dummyItem: lf.schema.Table = null;
    var inserts:number = 50000;
    
    schemaBuilder.connect({storeType: lf.schema.DataStoreType.WEB_SQL}).then(websql =>{
    schemaBuilder.connect({storeType: lf.schema.DataStoreType.INDEXED_DB}).then(indexeddb =>{
        sequence(
            [testSelectPredicate, indexeddb, 'IndexedDB'],
            [testSelectPredicate, websql, 'WebSQL'],
            [testSelect, indexeddb, 'IndexedDB'],
            [testSelect, websql, 'WebSQL'],
            [testInsert, indexeddb, 'IndexedDB'],
            [testInsert, websql, 'WebSQL'],
            [dropTable, indexeddb, 'IndexedDB'],
            [dropTable, websql, 'WebSQL'],
            [testInsertBatch, indexeddb, 'IndexedDB'],
            [testInsertBatch, websql, 'WebSQL'],
            [dropTable, indexeddb, 'IndexedDB'],
            [dropTable, websql, 'WebSQL']    
        );  
    })});
    
    function dropTable(db: lf.Database, storeType:string){ 

            var starttime:number = Date.now();                   
            dummyItem = db.getSchema().table('Item');
            return db.delete().from(dummyItem).exec().then(()=>{
                var duration = Date.now() - starttime;
                console.log(`lovefield:${storeType} deleting ${inserts} rows ${duration}ms`);
            })   
    }
    
    function testInsert(db: lf.Database, storeType: string) {        

        var starttime:number = Date.now();                      
        var dummyItem = db.getSchema().table('Item');
        
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
        return tx.exec(q).then(()=>{
            var duration = Date.now() - starttime;
            console.log(`lovefield:${storeType} ${inserts} inserts ${duration}ms`);                
        });

    }
    
    function testInsertBatch(db: lf.Database, storeType: string) {        

        var starttime:number = Date.now();            
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
        return db.insert().into(dummyItem).values(rows).exec().then(()=>{
            var duration = Date.now() - starttime;
            console.log(`lovefield:${storeType} 1 insert ${inserts} rows ${duration}ms`);               
        });
    }    
    
    
    function testSelectPredicate(db: lf.Database, storeType: string) {     
        
        var starttime:number = Date.now();        
        dummyItem = db.getSchema().table('Item');
        
        var tx = db.createTransaction();
        var q = [];
        
        for (var i=0; i<inserts; i++) {            
        var column: lf.schema.Column = (<any>dummyItem).id;
        var select = db.select().from(dummyItem).where(column.eq(10000));            
        q.push(select);
        }
        return tx.exec(q).then((r)=>{
                var duration = Date.now() - starttime;
                console.log(`lovefield:${storeType} ${inserts} selects single row ${duration}ms`);         
                if (r.length !== inserts) console.error('selected more rows than expected. Mabey a bug in db.delete()?'); 
        });
    }  

    function testSelect(db: lf.Database, storeType: string) {     
        var starttime:number = Date.now();            
        var dummyItem = db.getSchema().table('Item');                                          
        var select = db.select().from(dummyItem);            
                    
        return select.exec().then((rows)=>{
            if (!rows.length || rows.length < inserts) console.error( `lovefield:${storeType} did not select the expected number of rows!`);
            else {
                var duration = Date.now() - starttime;
                console.log(`lovefield:${storeType} 1 select ${inserts} rows ${duration}ms`);
            }              
        });
    }  
   
    
})();
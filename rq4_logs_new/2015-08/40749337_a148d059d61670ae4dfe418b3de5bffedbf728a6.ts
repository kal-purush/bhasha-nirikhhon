import dc = require("./lib/DataContext");
import dl = require("./lib/DataLayer");
import q = require("q");

var ctx = new dc.DataContext(new dl.Sqlite3DataLayer("test.db"));

var incrementLock = (item): q.Promise<any> => {
    return q.fcall((): void => {
        item.Locking = (item.Locking ? item.Locking + 1 : 1);
    });
};

var locks = ctx.createDataModel({
    name: "locks",
    isAbstract: true,
    columns: [
        { name: "Locking", dataType: dl.DataTypes.int }
    ],
    beforeInsertCallback: incrementLock,
    beforeUpdateCallback: incrementLock
})
var users = ctx.createDataModel({
    name: "users",
    columns: [
        { name: "Id", dataType: dl.DataTypes.int, isAutoIncrement: true, isPrimaryKey: true },
        { name: "FirstName", dataType: dl.DataTypes.text },
        { name: "LastName", dataType: dl.DataTypes.text },
        { name: "Email", dataType: dl.DataTypes.text },
        { name: "FullName", dataType: dl.DataTypes.text }
    ],
    beforeInsertCallback: (item): q.Promise<any> => {
        return q.fcall(() => {
            item.FullName = item.FirstName + " " + item.LastName;
        });
    }
}, locks);

var master = ctx.createDataModel({
    name: "Master",
    isAbstract: true,
    columns: [
        { name: "Id", dataType: dl.DataTypes.int, isPrimaryKey: true, isAutoIncrement: true }
    ]
});
var besuche = ctx.createDataModel({
    name: "Besuche",
    columns: [
        { name: "Von", dataType: dl.DataTypes.date },
        { name: "Bis", dataType: dl.DataTypes.date },
        { name: "Text", dataType: dl.DataTypes.text }
    ]
}, master);
var berichte = ctx.createDataModel({
    name: "Berichte",
    columns: [
        { name: "Text", dataType: dl.DataTypes.text },
        { name: "IdBesuch", dataType: dl.DataTypes.int, relation: { parentTable: besuche.tableInfo.table, parentAssociationName: "Besuch", childAssociationName: "Berichte" } }
    ]
}, master);

ctx.finalizeInitialize()
    .then((): q.Promise<any> => {
        console.log("Schema updated");

        return users.insertAndSelect({
            FirstName: "Stefan",
            LastName: "Heim",
            Email: "stefan.heim@hotmail.com"
        });
    })
    .then((r): q.Promise<any> => {
        console.log("Item inserted");
        r.FirstName = "Stefan NEU";
        return users.updateAndSelect(r);
    })
    .then((r): q.Promise<any> => {
        console.log("Item updated");
        return users.delete(r);
    })
    .then((r): q.Promise<any> => {
        return besuche.insertAndSelect({
            Von: new Date(),
            Bis: new Date(),
            Text: "Das ist ein Test"
        })
    })
    .then((r): q.Promise<any> => {
        return berichte.insertAndSelect({
            Text: "Das ist ein Test",
            IdBesuch: r.Id
        })
    })
    .then((r): q.Promise<any> => {
        console.log("Item deleted");
        return besuche.select({
            expand: ["Berichte"]
        })
    })
    .then((r): q.Promise<any> => {
        return besuche.updateOrInsert({
            Von: new Date(),
            Bis: new Date(),
            Text: "Spezial",
            Berichte: [
                { Text: "Spezial 1" },
                { Text: "Spezial 2" },
                { Text: "Spezial 3" }
            ]
        });
    })
    .then((r): void => {
        console.log(JSON.stringify(r, null, 2));
        console.log("All Tasks done");
    })
    .catch((r): void => {
        console.log(r);
    })
    .done();
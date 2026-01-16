import fs = require("fs");
import dl = require("../lib/DataLayer");
import dc = require("../lib/DataContext");

fs.unlinkSync("test.db");

var sqlite3 = new dl.Sqlite3DataLayer("test.db");
var dataCtx = new dc.DataContext(sqlite3);

var base = dataCtx.createDataModel({
    name: "base",
    isAbstract: true,
    columns: [
        { name: "Id", dataType: dl.DataTypes.int, isPrimaryKey: true, isAutoIncrement: true }
    ]
});
var users = dataCtx.createDataModel({
    name: "users",
    columns: [
        { name: "UserName", dataType: dl.DataTypes.text },
        { name: "FirstName", dataType: dl.DataTypes.text },
        { name: "LastName", dataType: dl.DataTypes.text },
        { name: "Email", dataType: dl.DataTypes.text }
    ]
}, base);
var profiles = dataCtx.createDataModel({
    name: "profiles",
    columns: [
        { name: "Name", dataType: dl.DataTypes.text }
    ]
}, base);
var usersToProfile = dataCtx.createDataModel({
    name: "users_to_profile",
    columns: [
        { name: "IdUser", dataType: dl.DataTypes.text, relation: { parentTable: users.tableInfo.table, parentAssociationName: "Profiles", childAssociationName: "User" } },
        { name: "IdProfile", dataType: dl.DataTypes.text, relation: { parentTable: profiles.tableInfo.table, parentAssociationName: "Users", childAssociationName: "Profile" } }
    ]
}, base);

var finalized = dataCtx.finalizeInitialize();

export = {
    base: base,
    users: users,
    profiles: profiles,
    usersToProfile: usersToProfile,
    dataCtx: dataCtx,
    finalized: finalized
}
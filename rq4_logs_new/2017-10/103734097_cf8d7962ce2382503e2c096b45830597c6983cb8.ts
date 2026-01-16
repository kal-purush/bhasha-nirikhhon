import * as SqlConnection from '../AppData/sql-connection';
var connection = SqlConnection.Connection();
import { DepartmentAc } from '../Models/user-model';

export let GetAllDepartment = (res: any) => {
    connection.query('SELECT * FROM nodemvc.departments;', function (err, rows, field) {
        res.send(rows);
    });
}

export let GetDepartmentById = (res: any, id: number) => {
    connection.query('SELECT * FROM nodemvc.departments WHERE Id = ?;', [id], function (err, rows, field) {
        res.send(rows[0]);
    });
}

export let AddDepartment = (res: any, department: DepartmentAc) => {
    let date = new Date();
    let dbDate = date.getFullYear() + "-" + (date.getMonth() + 1) + "-" + date.getDate();
    connection.query('INSERT INTO nodemvc.departments (name, createdAt, updatedAt)VALUES(?,?,?);',
        [department.name, dbDate, dbDate], function (err, rows, field) {
            res.send();
        });
}

export let EditDepartment = (res: any, department: DepartmentAc) => {
    let date = new Date();
    let dbDate = date.getFullYear() + "-" + (date.getMonth() + 1) + "-" + date.getDate();
    connection.query('UPDATE nodemvc.departments SET name=?, updatedAt=? WHERE Id = ?;',
        [department.name, dbDate, department.id], function (err, rows, field) {
            res.send();
        });
}
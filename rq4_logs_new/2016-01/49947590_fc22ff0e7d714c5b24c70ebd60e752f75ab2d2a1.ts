'use strict';

import {IConnectionAttributes, IConnection, IExecuteOptions, IExecuteReturn} from 'oracledb';
import * as OracleDB from 'oracledb';

class OraclePromisses {
	getConnection(opt: IConnectionAttributes): Promise<IConnection> {
		return new Promise<IConnection>((res, rej) => {
			OracleDB.getConnection(opt, (err, connection) => {
				return err ? rej(err) : res(connection);
			});
		});
	}

	query(sql:string, binds: Object | Array<any>, options: IExecuteOptions, conn): Promise<IExecuteReturn> {
		return new Promise<IExecuteReturn>((res, rej) => {
			conn.execute(sql, binds, options, (err, val) => {
				return err ? rej(err) : res(val);
			});
		});
	}

	getAllRows(exeRet: IExecuteReturn): Promise<Array<Array<any>> | Array<Object>> {
		return new Promise<Array<Array<any>> | Array<Object>>((res, rej) => {
			console.log('getRows', exeRet);
			if (exeRet.rows) {
				res(exeRet.rows);
			} else {
				let ret: Array<Array<any>> | Array<Object> = [];
				function populaRows(ret, rs) {
					rs.getRows(20, (err, rows) => {
						rows.forEach(val => ret.push(val));
						if (rows.length < 20) {
							rs.close((err) => {
								return err ? rej(err) : res(ret);
							});
						} else {
							populaRows(ret, rs);
						}
					});
				}
				populaRows(ret, exeRet.resultSet);
			}
		});
	}
}

class App {
	connAtt = {
		user: "rnjesus",
		password: "Bigous*123",
		connectString: "retecsp1tm.world"
	};
	ora = new OraclePromisses();

	run() {
		let opt: IExecuteOptions = {
			resultSet: true
		};
		this.ora.getConnection(this.connAtt)
			.then(this.ora.query.bind(this, 'select num_ele_int from cad_topo_int where cod_situ_int <> :situ and rownum <= :rn', ['RM', 20], opt))
			.then(this.ora.getAllRows)
			.then(console.log)
			.catch(console.error);
	}
}

new App().run();
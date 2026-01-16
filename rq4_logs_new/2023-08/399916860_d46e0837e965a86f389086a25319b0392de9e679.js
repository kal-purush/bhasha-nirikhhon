const fs = require('fs');

const CONNECTION_TYPE = {
    MYSQL: 'connection/mysql',
    FILE: 'connection/file',
    POSTGRES: 'connection/postgres',
};

const FIELD_TYPE = {
    INT: 'type/int',
    STRING: 'type/string',
    BIGINT: 'type/int',
    JSON: 'type/json',
    BOOLEAN: 'type/boolean',
};

const FIELD_META = {
    AUTO: 'meta/auto',
    REQUIRED: 'meta/required',
    FILTERED: 'meta/filtered',
};

const ORDER = {
    ASC: 'order/asc',
    DESC: 'order/desc',
};

let allowLog = true;

function setLog(log) {
    allowLog = log;
}

class Connection {
    constructor(prefix = null) {
        this.connection = null;
        this.prefix = prefix;
    }

    init() {
        return this.createConnection();
    }

    createConnection() {
        throw new Error("Connection must override createConnection");
    }
    
    async getConnection() {
        if (!this.connection) {
            this.log('connecting again');
            this.connection = await this.createConnection();
        }
        return this.connection;
    }

    log(message) {
        if (!allowLog) {
            return;
        }
        console.log(message);
    }

    getTable(table) {
        if (!this.prefix) {
            return table;
        }
        return `${this.prefix}_${table}`;
    }

    close() {
        throw new Error("Connection must override close");
    }

    async initializeTable(tableName, fields, version) {
        throw new Error("Connection must override initalizeTable");
    }

    async search(tableName, whereClause, order, limit) {
        throw new Error("Connection must override search");
    }

    async update(tableName, id, update) {
        throw new Error("Connection must override update");
    }

    async insert(tableName, tableFields, insertData) {
        throw new Error("Connection must override insert");
    }

    async delete(tableName, id) {
        throw new Error("Connection must override delete");
    }
}

Connection.fileConnection = async (cacheDir) => {
    const connection = new Connection(CONNECTION_TYPE.FILE, { cacheDir });
    await connection.init();
    return connection;
}

Connection.mysqlConnection = async ({ host, username, password, database, url }) => {
    const connection = new Connection(CONNECTION_TYPE.MYSQL, {
        host, user: username, password, database, url,
    });
    await connection.init();
    return connection;
}

Connection.postgresConnection = async ({ host, username, password, database, port, url }) => {
    const connection = new Connection(CONNECTION_TYPE.POSTGRES, {
        host, user: username, password, database, port, url,
    });
    await connection.init();
    return connection;
}

module.exports = {
    Connection,
    CONNECTION_TYPE,
    FIELD_TYPE,
    FIELD_META,
    ORDER,
    setLog,
}
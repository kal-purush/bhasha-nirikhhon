global.SALT_KEY = '7c31d4ae-d94b-4b85-b4af-88916207451a';
global.WELCOME_TEMPLATE = 'Olá {0}, <br> Esse é seu usuário: {1} e sua senha:{2} para acessar nosso site.';
global.SEND_GRID_KEY = "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"
global.TOKEN_NAME = 'authorization';
global.DATABASE_CONECT = {
    user: 'root',
    password: 'jc@1t',
    name: 'dbwebprotese'
};

global.DATABASE_DETAILS = {
    host: 'localhost',
    port:3306,
    dialect: 'mysql',
    pool: {
        max: 5,
        min: 0,
        acquire: 30000,
        idle: 10000
    },
    storage: 'path/to/database.sqlite',
    operatorsAliases: false
};
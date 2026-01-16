


export class DtoCreateBackup{

    public user:string;
    public password:string;
    public database:string;
    public server:string;

    constructor(user:string,password:string,database:string,server:string){
        this.user=user;
        this.password=password;
        this.database=database;
        this.server=server;    
    }


    static generate(body:{[key:string]:any}):DtoCreateBackup{
        return new DtoCreateBackup(body.user,body.password,body.database,body.server);
    }

}
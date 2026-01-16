import { IBackup } from "../interfaces/Ibackup";







export class Backup{

    private strategy:IBackup;

    constructor(strategy:IBackup){
        this.strategy=strategy;
    }


    setStrategy(newStrategy:IBackup):void{
        this.strategy=newStrategy;
    }


    async execute(user:string,password:string,dbname:string,serverName:string):Promise<string>{
        let fileGenerate = await this.strategy.generateBackup(user,password,dbname,serverName);
        return fileGenerate;
    }


}
import winston from 'winston';
import axios from 'axios';
import dotenv from "dotenv";

dotenv.config();

type LogLevel='error' | 'warn' | 'info' | 'debug';

interface LogParams{
    message:string;
    error?: Error | unknown;
    context?: Record<string,any>;
}

export class DataDogLogger{
    private readonly dataDogAPiUrl:string;
    private readonly dataDogApiKey:string;
    private logger:winston.Logger;

    constructor(){
        this.dataDogAPiUrl=process.env.DATADOG_API_URL || '';
        this.dataDogApiKey=process.env.DATADOG_API_KEY || '';

        
    // const httpTransportOptions = {
    //     host: 'http-intake.logs.datadoghq.com',
    //     path: '/api/v2/logs?dd-api-key=<DATADOG_API_KEY>&ddsource=nodejs&service=<APPLICATION_NAME>',
    //     ssl: true
    // };
        this.logger=winston.createLogger({
            level:'info',
            format:winston.format.json(),
            transports:[
                new winston.transports.Console({
                    format:winston.format.simple(),
                }),
            ],
        });
    }
    private async sendToDatadog(level:LogLevel,message:string,context:Record<string,any>={})
    {
        try{
            await axios.post(this.dataDogAPiUrl,{
                ddsource:'nodejs',
                service:'Assignment6',
                hostname:"local",
                status:level,
                message,
                context,
            },{
                headers:{
                    'Content-Type':'application/json',
                    'DD-API_KEY':this.dataDogApiKey,
                },
            });
        }
        catch(error)
        {
            console.error('Failed to send log to dataDog',error);
        }
    }
    public async log({message,error,context={}}:LogParams,level:LogLevel="info"){
        const logContext={
            error:error instanceof Error ? error.stack:error,
            timestamp:new Date().toISOString(),
            ...context,
        };
        this.logger.log(level,message,logContext);
        await this.sendToDatadog(level,message,logContext);
 }
   public async info(params:LogParams)
   {
    await this.log(params,'info');
   }
   public async error(params:LogParams)
   {
    await this.log(params,'error');
   }
   public async warn(params:LogParams)
   {
    await this.log(params,'warn');
   }
   public async debug(params:LogParams)
   {
    await this.log(params,'debug');
   }
}

// import { createLogger,transports,format } from "winston";

//     const httpTransportOptions = {
//         host: 'http-intake.logs.datadoghq.com',
//         path: '/api/v2/logs?dd-api-key=<55d19128b7e52fbf0dd0a911501f815a>&ddsource=nodejs&service=<Assignment6>',
//         ssl: true
//     };

// export const logger = createLogger({
//   level: 'info',
//   exitOnError: false,
//   format: format.json(),
//   transports: [
//     new transports.Http(httpTransportOptions),
//   ],
// });
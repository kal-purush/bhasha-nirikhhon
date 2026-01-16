declare namespace Express {
    export interface Request {
       user: {
         uid: string,
         name: string,
         email: string
       }
    }
 }
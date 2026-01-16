// src/global.d.ts
declare module "formidable" {
    export interface File {
      filepath: string;
      originalFilename: string;
      mimetype: string;
      size: number;
    }
  
    export interface Fields {
      [key: string]: string | string[];
    }
  
    export interface Files {
      [key: string]: File | File[];
    }
  
    export class IncomingForm {
      parse(req: any, callback: (err: any, fields: Fields, files: Files) => void): void;
    }
  
    const formidable: {
      IncomingForm: typeof IncomingForm;
    };
  
    export = formidable;
  }
  
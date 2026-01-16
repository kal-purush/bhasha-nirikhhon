import { Injectable } from '@nestjs/common';
import { readFileSync } from 'fs';
import {readFile, writeFile} from 'fs';
import { EntryPointDTO } from 'src/dtos/entry.dto';
@Injectable()
export class PersistanceService {
    filePath: string = 'src/mock/db.json';
    constructor() {}

    readData(): EntryPointDTO[] {
        let json = readFileSync(this.filePath,"utf8");
        console.log("ici")
        return json ===""?[]:JSON.parse(json);
    }

    addData(data:EntryPointDTO) {
        var db = this.readData();
        db.push(data);
        var newData = JSON.stringify(db, null, 2);
        writeFile(this.filePath, newData, (err) => {
            // Error checking
            if (err) throw err;
            console.log("New data added");
          });
    }

}
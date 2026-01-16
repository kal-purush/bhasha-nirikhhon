import { CallBack } from 'promise-lib';
import * as fs from 'fs';
import * as path from 'path';




function PackageJsonGetter(root?:string){
    let start = path.resolve(root||__dirname);

    const loop_up = async ()=>{
        let [err,listing] = await CallBack.Call(fs.readdir,start) as [Error,string[]];
        if(err){
            throw err;
        }
        if(listing.find(file=>file === "package.json")){
            return start;
        }else{
            let newstart = path.resolve(start,"..");

            if(newstart!==start){
                start=newstart;
                return loop_up();
            }else{
                return null;
            }
        }
    }
}

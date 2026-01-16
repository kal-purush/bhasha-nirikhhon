import * as sinon from "sinon";
import * as chai from "chai";
import * as os from 'os';
import * as path from 'path';
import * as uuid from "uuid/v1";
import { FileCreator } from '../src/Framework/Core/Components/FileSystem/FileCreator';
import * as fs from 'fs';
import { CallBack } from 'promise-lib';
const expect = chai.expect;



describe("FileCreator",function(){

    let filepath = path.join(os.tmpdir(),uuid());

    before(async function(){
        let fc = new FileCreator();
        await fc.create(filepath);
    })


    it("should be able to create a file",async function(){

        let [err,stat]:[Error,fs.Stats] = await CallBack.Call(fs.stat,filepath);
        if(err) throw err;
        expect(stat.isFile()).to.be.true;
        
    })


    after(async function(){
        fs.unlinkSync(filepath);
    })
})
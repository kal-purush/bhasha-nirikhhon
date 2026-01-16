import * as sinon from "sinon";
import * as chai from "chai";
import { CallBack } from 'promise-lib';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as uuid from 'uuid/v1';
import { FileRemover } from '../src/Framework/Core/Components/FileSystem/FileRemover';

const expect = chai.expect;



describe("FileRemover",function(){

    const filename = path.join(os.tmpdir(),uuid());
    const fr = new FileRemover();

    before(async function(){
        fs.writeFileSync(filename,"");
    })


    it("should be able to delete the file",async function(){
        await fr.delete(filename);
        let [err,stat] = await CallBack.Call(fs.stat,filename)
        expect(err).to.exist;
        expect(stat).to.not.exist;
    })




    after(async function(){
        
    })
})
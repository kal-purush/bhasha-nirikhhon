

import * as sinon from "sinon";
import * as chai from "chai";
import PackageJsonGetter from '../Framework/Utilities/Getters/PackageJsonGetter';
import * as path from 'path';

const expect = chai.expect;



describe("PackageJsonGetter",function(){

    it("should be able find the package.json file of the project",async function(){
        let result = await PackageJsonGetter();
        let expected = path.resolve(__dirname,"../package.json");
        expect(result).to.equal(expected);
    })


    after(async function(){
        
    })
})


import * as sinon from "sinon";
import * as chai from "chai";
import { DecoratorBuilder } from '../src/Framework/Core/Components/Decorators/DecoratorBuilder';
import { Accessor } from '../lib/Accessor/Accessor';

const expect = chai.expect;





describe("Test",function(){

    const builder = new DecoratorBuilder();

    

    before(async function(){
        builder.Construct = function<T extends Function>(_class:T){
            expect(arguments.length ===1 )
        }


        builder.Method =  function(proto,key:string,pd:PropertyDescriptor){
            pd.value = function(){
                return "hello world"
            }
            return pd;
        }
    })


    it("should",async function(){
        let decorator = await builder.build();

        
        class Test{
            @decorator
            method(){
                return null;
            }
        }

    })


    after(async function(){
        
    })
})
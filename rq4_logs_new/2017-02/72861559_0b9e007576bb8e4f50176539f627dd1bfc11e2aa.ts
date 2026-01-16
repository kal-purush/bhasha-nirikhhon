import * as sinon from "sinon";
import * as chai from "chai";
import { MainController } from '../Framework/Core/Components/Controllers/MainController';
import Get from '../Framework/Core/Components/Routing/Methods/Get';
import GetControllerMethodOptions from '../Framework/Core/Components/Controllers/GetControllerMethodOptions';
import { ControllerMethodOptions } from '../Framework/Core/Components/Controllers/Options/ControllerMethodOptions';

const expect = chai.expect;



describe("Get Decorator",function(){


    class TestController extends MainController{

        @Get("/test")
        test_method(){

        }
    }

    let method_options:ControllerMethodOptions<MainController>[];
    let method_option:ControllerMethodOptions<MainController>;
    before(async function(){
        method_options = GetControllerMethodOptions().filter(o=>{
            return o.klass === TestController
        });

    })


    it("should be able to get created method_options",async function(){
        expect(method_options).to.have.lengthOf(1);
        method_option = method_options[0];
    })

    it("should be able to get method name",function(){
        expect(method_option.method_name).to.equal("test_method");
    })

    it("should be able to get method path",function(){
        expect(method_option.method_path).to.equal("/test");
    })

    it("should be able to get method type",function(){
        expect(method_option.method_type).to.equal("GET");
    })


    after(async function(){
        
    })
})
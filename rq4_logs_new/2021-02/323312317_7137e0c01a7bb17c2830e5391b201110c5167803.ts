import container, { IContainer } from "../src/container";
import chai from 'chai';

describe('container', () => {
    it('Make sure all dependencies can be resolved', () => {

        // container.dbHandler.connect({
        //     dbName: "",
        //     uri: "",
        //     ssl: false
        // });
        Object.keys(container).forEach(x => {
            try {
                const k = x as keyof IContainer;
                const d = container[k];
            }catch(e){
                chai.assert(false, e.message);
            }
        })
    })
})
import Vue from 'vue';
import Col from '../src/col';

const expect = chai.expect;

Vue.config.productionTip = false;
Vue.config.devtools = false;

describe('Col', () => {
    it('存在.', () => {
        expect(Col).to.be.ok;
    });
    describe('测试属性',()=>{
        it('设置span=2 会出现 col-span-2类名',()=>{
            let divElement = document.createElement('div');
            const Constructor = Vue.extend(Col);
            let vm = new Constructor({
                propsData:{
                    span:'2',
                }
            })
            vm.$mount(divElement);
            expect(vm.$el.classList.contains('col-span-2')).to.be.true;
            vm.$destroy()
        })
        it('设置offset=2 会出现 col-offset-2类名',()=>{
            let divElement = document.createElement('div');
            const Constructor = Vue.extend(Col);
            let vm = new Constructor({
                propsData:{
                    offset:'2',
                }
            })
            vm.$mount(divElement);
            expect(vm.$el.classList.contains('col-offset-2')).to.be.true;
            vm.$destroy()
        })
    })


});
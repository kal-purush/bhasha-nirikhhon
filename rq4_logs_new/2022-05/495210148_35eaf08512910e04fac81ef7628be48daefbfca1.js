import { mount } from '@vue/test-utils';
import CardResult from '@/components/home/CardResult.vue';

function mountComponent(propsData) {
    return mount(CardResult, { propsData });
}

describe('CardResult.vue', () => {
    it('should init default property with value 1', async () => {
        const wrapper = await mountComponent();

        expect(wrapper.vm.default).toEqual(1);
    });
});
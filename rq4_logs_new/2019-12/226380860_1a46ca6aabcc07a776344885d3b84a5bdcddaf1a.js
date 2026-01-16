import List from "../../src/components/List";
import Item from "../../src/models/Item";
import {mount} from "@vue/test-utils";

describe('List component', () => {
    const testData = [new Item("Test task")];
    const wrapper = mount(List, {
        propsData: {
            list: testData
        }
    });


    it('Check if item is updated correctly when the item in the list is marked as done', () => {
        expect(testData[0].done).toBe(false);
        wrapper.find('.list-item > div > span').trigger('click');
        expect(testData[0].done).toBe(true);
    })
});


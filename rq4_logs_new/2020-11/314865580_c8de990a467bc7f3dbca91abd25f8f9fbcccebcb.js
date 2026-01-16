import { mount } from '@vue/test-utils';
import Button from './Button.vue';

describe('Button', () => {
  test('renders correctly', () => {
    const wrapper = mount(Button, {
      slots: {
        default: 'Thuumper',
      },
    });
    expect(wrapper.element).toMatchSnapshot();
  });

  test('renders the component as a button by default', () => {
    const wrapper = mount(Button, {
      slots: {
        default: 'Thuumper',
      },
    });
    expect(wrapper.element.tagName).toBe('BUTTON');
  });

  test('transparently passes through classes and attributes', () => {
    const wrapper = mount(Button, {
      slots: {
        default: 'Thuumperß',
      },
      attrs: {
        id: 'test-button',
        type: 'reset',
        disabled: true,
      },
    });
    expect(wrapper.element.id).toBe('test-button');
    expect(wrapper.element.type).toBe('reset');
    expect(wrapper.element.disabled).toBe(true);
  });
});
import React from 'react';
import '@testing-library/jest-dom';
import { GifExpertApp } from '../GifExpertApp';
import { shallow } from 'enzyme';

describe('Probando componente <GifExpertApp />', () => {
    test('Debe hacer render el componente correctamente', () => {
        const wrapper = shallow(<GifExpertApp />);
        expect(wrapper).toMatchSnapshot();
    });
    test('debe mostrar una lista de categorias', () => {
        const categories = ['goku', 'pokemon', 'hola'];
        const wrapper = shallow(<GifExpertApp defaultCategories={categories} />);
        expect(wrapper).toMatchSnapshot();
        expect(wrapper.find('GifGrid').length).toBe(3);
    })


})
import React from 'react';
import { mount } from 'enzyme';
import { MemoryRouter, Route } from 'react-router';
import { SearchScreen } from '../../../components/search/SearchScreen';

describe('Puebas en <SearchScreen />', () => {
	test('debe de mostrarse correctamente con valores por defecto', () => {
		const wrapper = mount(
			<MemoryRouter initialEntries={['/search']}>
				<Route path="/search" component={SearchScreen} />
			</MemoryRouter>
		);
		expect(wrapper).toMatchSnapshot();
		expect(wrapper.find('.alert-info').text().trim()).toBe('Search a hero');
	});

	test('debe de mostrar a Batman y el input con el valor del queryString', () => {
		const wrapper = mount(
			<MemoryRouter initialEntries={['/search?q=batman']}>
				<Route path="/search" component={SearchScreen} />
			</MemoryRouter>
		);
		expect(wrapper.find('input').prop('value')).toBe('batman');
		expect(wrapper).toMatchSnapshot();
	});
});
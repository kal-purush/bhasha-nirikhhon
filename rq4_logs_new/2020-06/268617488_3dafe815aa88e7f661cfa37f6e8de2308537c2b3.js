import React from 'react';

import { mountWithProvider, actWait } from '../helpers';
import {productShowCaseMocks, productShowCasErrorMocks } from '../mocks';

import ProductList from '../../pages/ProductList';
import Card from '../../components/Card';

import Loader from '../../components/Loader';
import Error from '../../components/Error';

it('Product List renders without error', () => {
    const wrapper = mountWithProvider(<ProductList />,productShowCaseMocks)
    expect(wrapper.find(ProductList)).toHaveLength(1);
});

it('Product List should render loading state initially', async () => {
    const wrapper = mountWithProvider(<ProductList />,[]);
    await actWait(0);
    expect(wrapper.find(Loader)).toHaveLength(1);
});

it('Product List should render success', async () => {
    const wrapper = mountWithProvider(<ProductList />, productShowCaseMocks)
    await actWait(0);
    wrapper.update();
    expect(wrapper.find(Card)).toHaveLength(1)
});

it('Product List should render error', async () => {
    const wrapper = mountWithProvider(<ProductList />, productShowCasErrorMocks)
    await actWait(0);
    wrapper.update();
    const errorWrapper = wrapper.find(Error);
    expect(errorWrapper).toHaveLength(1);
});

it('Product List should show error UI', async () => {
    const wrapper = mountWithProvider(<ProductList />, productShowCasErrorMocks)
    await actWait(0);
    wrapper.update();
    const errorWrapper = wrapper.find(Error);
    expect(errorWrapper.prop('message')).toEqual("Network error: Error!");
})
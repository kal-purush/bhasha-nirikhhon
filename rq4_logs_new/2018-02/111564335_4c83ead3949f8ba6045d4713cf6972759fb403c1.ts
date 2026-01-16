
import React from 'react';
import moment from 'moment';
import countries from 'i18n-iso-countries';
import CountryList from './CountryList';
import CountryListElement from './CountryListElement';

describe('<CountryList />', () => {
    it('should render an add button', () => {
    const wrapper = shallow(<CountryList />);
    
    });

    it('should render all visits', () => {
        const listOfCountries = [
            {
                land: 'DK',
                startDato: moment().toISOString(),
                sluttDato: moment().toISOString()
            },
            {
                land: 'AF',
                startDato: moment().toISOString(),
                sluttDato: moment().toISOString()
            }
        ];
        const wrapper = shallow(<CountryList visits= {listOfCountries} / > );
        expect(wrapper.find(CountryListElement)).to.have.lengthOf(
            listOfCountries.length
        );
    });

    it('should render flags for all countries', () => {
        const listOfCountries = [
            {
                land: 'DK',
                startDato: moment().toISOString(),
                sluttDato: moment().toISOString()
            },
            {
                land: 'AF',
                startDato: moment().toISOString(),
                sluttDato: moment().toISOString()
            }
        ];
        const wrapper = shallow(<CountryList visits= {listOfCountries} / > );
        console.log(wrapper.debug());
        console.log(test.debug());
    });
});
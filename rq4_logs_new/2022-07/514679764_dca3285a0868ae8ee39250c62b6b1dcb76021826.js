import { retriveCountries } from "../api";

export const getCountryByRegion = (region) => {

    const validRegion = ['Africa', 'America', 'Asia', 'Europe', 'Oceania'];
    
    if(!validRegion.includes(region) ){
        throw new Error(`${region} is not a valid region`)
    }

    return retriveCountries.filter(country => country.region === region)

}
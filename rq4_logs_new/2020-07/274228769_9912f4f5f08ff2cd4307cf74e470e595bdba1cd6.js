import Api from "../api/Api";

class ZipCodeService {
    getAddressByZipCode = (zipCode) => {
        return Api.get('/zipCode/' + zipCode)
    }
}

export default ZipCodeService
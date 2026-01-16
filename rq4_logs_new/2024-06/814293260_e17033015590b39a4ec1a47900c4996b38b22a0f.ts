import superagent from "superagent";
import request from 'superagent';
import { apiData } from "../pages/authPage";
import { urlCompany } from "../pages/companyPage";
import { generateRequestData } from "../pages/companyPage";
const requestData = generateRequestData();
let token: string | undefined;


describe("Company module Tests", () => {
    test("Auth", async () => {
        const postRequest ={
            url: apiData.tokenUrl,
            method: 'POST',
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded'
            },
            form: {
                grant_type: 'password',
                client_id: 'captain-fe',
                username: apiData.username,
                password: apiData.password
            }
        };
        const res = await request
            .post(postRequest.url)
            .type('form')
            .send(postRequest.form);
        
        expect(res.status).toEqual(200);
        //добавить проверок на поля

        const responseJson = res.body;
        token = responseJson['access_token'];
    });

    test("Add Company", async () => {
        if (!token) {
            throw new Error("Access token is missing");
        }

        const res = await request
            .post(urlCompany.addCompanyUrl)
            .set('Authorization', `Bearer ${token}`)
            .send(requestData);
            let jsonData = res.body;
            console.log(jsonData);
        
        //Basic Checks
        expect(res.status).toEqual(200);
        expect(res.type).toMatch(/json/);
        expect(typeof jsonData.id).toBe('number');
        expect(typeof jsonData.createdAt).toBe('number');
        expect(typeof jsonData.updatedAt).toBe('number');

        //CompanyShortName
        expect(requestData.companyShortName).toEqual(jsonData.companyShortName);
        expect(typeof jsonData.companyShortName).toBe('string');


        //companyLegalName
        expect(requestData.companyLegalName).toEqual(jsonData.companyLegalName);
        expect(typeof jsonData.companyLegalName).toBe('string');
       
        //phoneNumber
        expect(requestData.phoneNumber).toEqual(jsonData.phoneNumber);
        expect(typeof jsonData.phoneNumber).toBe('string');
        expect(requestData.phoneNumber).toHaveLength(11);

        //e-mail
        expect((requestData.email).toLowerCase()).toEqual(jsonData.email);
        expect(typeof jsonData.email).toBe('string');

        //countryId
        expect(requestData.countryId).toEqual(jsonData.country.id);
        expect(typeof jsonData.country.id).toBe('number');
        
        //countryName
        expect(typeof jsonData.country.name).toBe('string');
        expect(jsonData.country.name).toEqual('Poland');
        
        //legalAddress
        expect(requestData.legalAddress.address).toEqual(jsonData.legalAddress.address);
        expect(typeof jsonData.legalAddress.address).toBe('string');
        //legalAddress - City
        expect(requestData.legalAddress.cityId).toEqual(jsonData.legalAddress.city.id);
        expect(typeof jsonData.legalAddress.city.id).toBe('number');

        //legalAddress - PostCode
        expect(requestData.legalAddress.postcode).toEqual(jsonData.legalAddress.postcode);
        expect(typeof jsonData.legalAddress.postcode).toBe('string');
        

        //correspondenceAddress
        expect(requestData.correspondenceAddress.address).toEqual(jsonData.correspondenceAddress.address);
        expect(typeof jsonData.correspondenceAddress.address).toBe('string');
        //correspondenceAddress - City
        // expect(requestData.correspondenceAddress.cityId).toEqual(jsonData.correspondenceAddress.city.id);
        expect(typeof jsonData.correspondenceAddress.city.id).toBe('number');
        //correspondenceAddress - PostCode
        expect(requestData.correspondenceAddress.postcode).toEqual(jsonData.correspondenceAddress.postcode);
        expect(typeof jsonData.correspondenceAddress.postcode).toBe('string');

        //companyRegistrationNumbers - REGON
        expect(jsonData.companyRegistrationNumbers[0].type.name).toBe('REGON');
        expect(requestData.companyRegistrationNumbers[0].number).toBe(jsonData.companyRegistrationNumbers[1].number);
        expect(jsonData.companyRegistrationNumbers[0].number).toHaveLength(14);

        //companyRegistrationNumbers - NIP
        expect(jsonData.companyRegistrationNumbers[1].type.name).toBe('NIP');
        expect(requestData.companyRegistrationNumbers[1].number).toBe(jsonData.companyRegistrationNumbers[0].number);
        expect(jsonData.companyRegistrationNumbers[1].number).toHaveLength(10);

    //Сheck comment
    expect(requestData.comment).toEqual(jsonData.comment);
    expect(typeof jsonData.comment).toBe('string');

    // Check isBlocked, isActive, isArchived(boolean values)
    expect(jsonData.isBlocked).toBe(false);
    expect(jsonData.isActive).toBe(true);
    expect(jsonData.isArchived).toBe(false);


    });
});

import request from "superagent";
import { urlCompany } from "../pages/companyPage";
import { companyId } from "./2_addCompany.tests";
import { tokenPlatformOwner } from "./1_auth.tests";

describe ("Active\Inactive Company", () => {
    test("Inactive Company Test", async () => {
        if (!tokenPlatformOwner) { 
            throw new Error("Access token is missing");
        }
        const res = await request
        .get(`${urlCompany.mainUrl}${urlCompany.companyUrl}/${companyId}/inactive`)
        .set("Authorization", `Bearer ${tokenPlatformOwner}`)

        //Basic Checks
        expect(res.status).toEqual(200);
        expect(res.body).toEqual({});
    });

    test("Check archived company Status", async () => {
        const res = await request
            .get(`${urlCompany.mainUrl}${urlCompany.companyUrl}/${companyId}`)
            .set("Authorization", `Bearer ${tokenPlatformOwner}`);
        let jsonData = res.body;

        //Basic Checks
        expect(res.status).toEqual(200);
        expect(res.type).toMatch(/json/);
        expect(typeof jsonData.id).toBe("number");
        expect(typeof jsonData.createdAt).toBe("number");
        expect(typeof jsonData.updatedAt).toBe("number");

        //CompanyShortName
        expect(jsonData.companyShortName).toBeTruthy();
        expect(typeof jsonData.companyShortName).toBe("string");

        //companyLegalName
        expect(jsonData.companyLegalName).toBeTruthy();
        expect(typeof jsonData.companyLegalName).toBe("string");

        //phoneNumber
        expect(jsonData.phoneNumber).toBeTruthy();
        expect(typeof jsonData.phoneNumber).toBe("string");
        expect(jsonData.phoneNumber).toHaveLength(11);

        //e-mail
        expect(jsonData.email).toBeTruthy();
        expect(typeof jsonData.email).toBe("string");

        //countryId
        expect(jsonData.country.Id).not.toBeNull();
        expect(typeof jsonData.country.id).toBe("number");

        //countryName
        expect(typeof jsonData.country.name).toBe("string");
        expect(jsonData.country.name).toBeTruthy();

        //legalAddress
        expect(jsonData.legalAddress.address).toBeTruthy();
        expect(typeof jsonData.legalAddress.address).toBe("string");
        //legalAddress - City
        expect(jsonData.legalAddress.city.Id).not.toBeNull();
        expect(typeof jsonData.legalAddress.city.id).toBe("number");

        //legalAddress - PostCode
        expect(jsonData.legalAddress.postcode).toBeTruthy();
        expect(typeof jsonData.legalAddress.postcode).toBe("string");

        //correspondenceAddress
        expect(jsonData.correspondenceAddress.address).toBeTruthy();
        expect(typeof jsonData.correspondenceAddress.address).toBe("string");

        //correspondenceAddress - City
        expect(jsonData.correspondenceAddress.city.Id).not.toBeNull();
        expect(typeof jsonData.correspondenceAddress.city.id).toBe("number");
        //correspondenceAddress - PostCode
        expect(jsonData.correspondenceAddress.postcode).toBeTruthy();
        expect(typeof jsonData.correspondenceAddress.postcode).toBe("string");

        //companyRegistrationNumbers - REGON
        expect(jsonData.companyRegistrationNumbers[0].type.name).toBe("REGON");
        expect(jsonData.companyRegistrationNumbers[0].number).toBeTruthy();
        expect(jsonData.companyRegistrationNumbers[0].number).toHaveLength(14);

        //companyRegistrationNumbers - NIP
        expect(jsonData.companyRegistrationNumbers[1].type.name).toBe("NIP");
        expect(jsonData.companyRegistrationNumbers[1].number).toBeTruthy();
        expect(jsonData.companyRegistrationNumbers[1].number).toHaveLength(10);

        //Сheck comment
        expect(jsonData.comment).toBeTruthy();
        expect(typeof jsonData.comment).toBe("string");

        // Check isBlocked, isActive, isArchived(boolean values)
        expect(jsonData.isBlocked).toBe(false);
        expect(jsonData.isActive).toBe(false);
        expect(jsonData.isArchived).toBe(false);
    });

})
import superagent from "superagent";
import request from 'superagent';
import { apiData } from "../pages/authPage";
import { urlCompany } from "../pages/companyPage";
import { generateRequestData } from "../pages/companyPage";
const requestData = generateRequestData();
let token: string | undefined;

describe("Users Test", () => {
    // test("Get All Users", async () => {
    //     const usersUrl = await superagent.get("https://reqres.in/api/users?page=2");
    //     expect(usersUrl.statusCode).toEqual(200);
    // });
    test("Auth First", async () => {
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


      const responseJson = res.body;
      console.log('ACCESS_TOKEN:', responseJson['access_token']);
    });

    test("Add Company", async () => {
      const res = await request
        .post(urlCompany.addCompanyUrl)
        .set('Authorization', `Bearer ${token}`)
        .send(requestData);
      expect(res.status).toEqual(200);
    });
});


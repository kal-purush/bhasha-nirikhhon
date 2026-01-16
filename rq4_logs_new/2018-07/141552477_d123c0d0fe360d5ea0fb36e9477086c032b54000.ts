import { User, jsonToUser } from "./index";

describe("jsonToUser", function () {

  test("The json is converted to User", function () {
    const userStr = `{ 
      "id": 1, 
      "userId": "admin", 
      "name": "admin admin", 
      "roleType": 1, 
      "lang": "ja", 
      "mailAddress": "eguchi@nulab.example" 
    }`
    const json = JSON.parse(userStr)
    const user = jsonToUser(json) as User

    expect(user.id).toEqual(1)
    expect(user.userId).toEqual("admin")
    expect(user.name).toEqual("admin admin")
    expect(user.mailAddress).toEqual("eguchi@nulab.example")
  })
})
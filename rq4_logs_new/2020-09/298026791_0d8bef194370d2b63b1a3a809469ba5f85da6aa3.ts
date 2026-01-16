import { Model, schema } from "..";

describe("Schema", () => {
    interface IUser {
        name: {
            first: string;
            last: string;
        }
    }

    class User extends Model<IUser> {
        static schema = {
            name: {
                first: schema.string().required(),
                last: schema.string().required()
            }
        }
    }
    describe("should validate", () => {
        it("it will be valid", () => {
            User.validateSchema({name: {first: "Adil", last: "Hello"}})
        })
        it("it will be invalid", () => {
            User.validateSchema(
                {name: { first: "Adil" }}, true
            )
        })
    })
})
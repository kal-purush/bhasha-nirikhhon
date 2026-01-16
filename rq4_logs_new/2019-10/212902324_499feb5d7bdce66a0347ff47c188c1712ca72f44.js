const graphql = require('graphql');
const axios = require("axios");

const {
    GraphQLObjectType,
    GraphQLString,
    GraphQLInt,
    GraphQLSchema,
    GraphQLNonNull,
    GraphQLList
} = graphql;

const CompanyType = new GraphQLObjectType({
    name: 'Company',
    fields: () => ({
        id: { type: GraphQLString },
        name: { type: GraphQLString },
        description: { type: GraphQLString },
        users: {
            type: GraphQLList(UserType),
            resolve(_, args) {
                const { id } = _;
                return axios(`http://localhost:3000/companies/${id}/users`).then((response) => response.data);
            }
        }
    })
});

const UserType = new GraphQLObjectType({
    name: 'User',
    fields: () => ({
        id: { type: GraphQLString },
        firstName: { type: GraphQLString },
        age: { type: GraphQLInt },
        nickName: { type: GraphQLString },
        company: {
            type: CompanyType,
            resolve(_, args) {
                const { companyId } = _;
                return axios(`http://localhost:3000/companies/${companyId}`).then((response) => response.data);
            }
        }
    })
});

const RootQuery = new GraphQLObjectType({
    name: 'RootQueryType',
    fields: {
        user: {
            type: UserType,
            args: { id: { type: GraphQLString } },
            resolve(_, args) {
                const { id } = args;
                return axios(`http://localhost:3000/users/${id}`).then((response) => response.data);
            }
        },
        company: {
            type: CompanyType,
            args: { id: { type: GraphQLString } },
            resolve(_, args) {
                const { id } = args;
                return axios(`http://localhost:3000/companies/${id}`).then((response) => response.data);
            }
        }
    },
});

const mutation = new GraphQLObjectType({
    name: 'Mutation',
    fields: {
        addUser: {
            type: UserType,
            args: {
                id: { type: GraphQLString },
                firstName: { type: GraphQLString },
                age: { type: GraphQLInt },
                nickName: { type: GraphQLString },
                companyId: { type: GraphQLString }
            },
            resolve() {

            }
        }
    }
});

module.exports = new GraphQLSchema({
    query: RootQuery,
});
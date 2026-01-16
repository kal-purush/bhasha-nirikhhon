const { ApolloServer, gql } = require('apollo-server-lambda')
const faunadb = require('faunadb')
const q = faunadb.query;


const typeDefs = gql`
  type Query {
    read: [Crud!]
  }
  type Crud {
    id: ID!
    name : String!
    description : String!
    image : String!
    isAvailable : Boolean!
  }

  type Mutation {
    addCrud ( name : String! description : String! image : String! isAvailable : Boolean!) : Crud ,
    removeCrud (id: ID!) : Crud ,
    updateCrud (id:ID! ,name : String! description : String! image : String! isAvailable : Boolean!) : Crud 

  }
`


const resolvers = {
  Query: {
    read: async (root, args, context) => {
      var client = new faunadb.Client({ secret: 'fnAD5v-7DFACBfjM6CvXoRbLzg6wcdXBbl4rglnb' });
      try {

        var result = await client.query(
          q.Map(
            q.Paginate(q.Documents(q.Collection('crud'))),
            q.Lambda(x => q.Get(x))
          )
          /*q.Map(
            q.Paginate(
              q.Match(
                q.Index('name')
              )
            ),
            q.Lambda(x => q.Get(x))
          ) */
        )
        console.log('Result  ', result)
        return result.data.map(d => {
          return {
            id: d.ref.id,
            name: d.data.name,
            description: d.data.description,
            image: d.data.image,
            isAvailable: d.data.isAvailable
          }
        })

      }
      catch (error) {
        console.log('Error ', error)
      }
    },
  },

  Mutation: {
    addCrud: async (_, args) => {
      var client = new faunadb.Client({ secret: 'fnAD5v-7DFACBfjM6CvXoRbLzg6wcdXBbl4rglnb' });
      try {
        const result = await client.query(
          q.Create(q.Collection("crud"), {
            data: args
          })
        );
        return result.data;
      }
      catch (error) {
        console.log("Error : ", error)
      }
    } ,
    removeCrud : async (_,args) => {
      var client = new faunadb.Client({ secret: 'fnAD5v-7DFACBfjM6CvXoRbLzg6wcdXBbl4rglnb'  });
      try{

        var result = await client.query(
          q.Delete( q.Ref(q.Collection('crud') , args.id)))
        console.log("Document Deleted : " , result.ref.id)
        return result.ref.data
      }
      
      catch (error) {
        console.log('Id inside Bookmark :' , id)
        console.log('error ' , error)
      }

    } ,
    updateCrud : async(_,args) => {
      var client = new faunadb.Client({ secret: 'fnAD5v-7DFACBfjM6CvXoRbLzg6wcdXBbl4rglnb'  });
      try{

        var result = await client.query(
          q.Update(
            q.Ref(q.Collection('crud'), args.id),
            { data: 
              { name: args.name,
                description : args.description,
                image : args.image,
                isAvailable : args.isAvailable
            
              } },
          )
          
          )
        console.log("Document Updated : " , result.ref.id)
        return result.ref.data
      }
      
      catch (error) {
        console.log('Id inside Bookmark :' , id)
        console.log('error ' , error)
      }
    }

  }
}


const server = new ApolloServer({
  typeDefs,
  resolvers,
})

exports.handler = server.createHandler()
export const resolvers = {
    Query: {
        say(root:any, args: any, context: any) {
            //abstracted layer that will get the data.
            return 'hello world';
        }
    }
};
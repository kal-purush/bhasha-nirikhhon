export const Subscription = {
    message: {
        subscribe: async (parent, args, ctx, info) => {
            return ctx.db.$subscribe
                .message({
                    where: {
                        mutation_in: ['CREATED', 'UPDATED']
                    }
                })
                .node();
        },
        resolve: payload => {
            return payload;
        }
    }
};
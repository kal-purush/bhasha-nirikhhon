import { Resolvers } from '../../../types/resolvers';
import {
    EmailSignInMutationArgs,
    EmailSignInResponse
} from '../../../types/graph';
import User from '../../../entities/User';

const resolvers: Resolvers = {
    Mutation: {
        EmailSignIn: async (
            _,
            args: EmailSignInMutationArgs
        ): Promise<EmailSignInResponse> => {
            const { email, password } = args;
            try {
                const user = await User.findOne({ email });
                if (!user) {
                    return {
                        ok: false,
                        error: 'No User found with that Email',
                        token: 'Common Soon'
                    };
                }
                const checkPassword = await user.comparePassword(password);
                if (checkPassword) {
                    return {
                        ok: true,
                        error: null,
                        token: null
                    };
                } else {
                    return {
                        ok: false,
                        error: 'Wrong password',
                        token: null
                    };
                }
            } catch (error) {
                return {
                    ok: false,
                    error: error.message,
                    token: null
                };
            }
        }
    }
};

export default resolvers;
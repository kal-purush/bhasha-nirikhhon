import mongoose from 'mongoose';
import { AccountModel } from '~/utils/db/models';

mongoose.connect(process.env.MONGO_DB_URI || 'mongodb+srv://localhost:27017').then(() => console.log('connected'));
export default defineEventHandler(async(event) => {
    const id = event.context.params.id;
    try {
        const account = await AccountModel.findOne({userId: id});
        if (!account) {
            return {
                error: 'Account not found'
            }
        }
        return {
            account
        }
    } catch (e) {
        return {
            error: 'The ID specified is invalid'
        }
    }

})
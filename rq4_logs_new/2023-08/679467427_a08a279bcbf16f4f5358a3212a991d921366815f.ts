import { Schema, model} from 'mongoose'


interface IUser{
    id: number;
    name: string;
    email: string;
    credential: string;// client | admin | other
    password: string;
};

const userSchema = new Schema<IUser>({
    id: { type: Number, required: true},
    name: { type: String, required: true},
    email: { type: String, required: true},
    credential: { type: String, required: true},
    password: { type: String, required: true, default: 'client'},//every new user is a client by default. Will be usefull when we need to create the routes
},
{
   timestamps: true, 
});

const User = model<IUser>('User', userSchema);

export default User;
import mongoose from "mongoose";
import { Password } from "../utils/password";

// interface que descreve as propiedades para criar um novo usuario
interface UserAttrs {
    email: string;
    password: string;
}

// interface que descreve as propiedades que o User model tem
// insere build como uma funçao em User
interface UserModel extends mongoose.Model<UserDoc> {
    build(attrs: UserAttrs): UserDoc;
}

// interface que descreve as propiedades que o documento User tem ao ser utilizado
interface UserDoc extends mongoose.Document {
    email: string;
    password: string;
}

const userSchema = new mongoose.Schema({
    email: {
        type: String,
        required: true
    },
    password: {
        type: String,
        required: true,
        
    }
},{
    toJSON: {
        transform(doc, ret) {
            ret.id = ret._id;
            delete ret._id
            delete ret.password;
            delete ret.__v;
        }
    }
});

// usar methods ou statics ?
userSchema.statics.build = (attrs: UserAttrs) => {
    return new User(attrs)
}

userSchema.pre('save', async function(done) {
    if (this.isModified('password')) {
        const hashed = await Password.toHash(this.get('password'))
        this.set('password', hashed)

    }
    done()
})

const User = mongoose.model<UserDoc, UserModel>('User', userSchema)


// testar metodo abaixo para criar usuario com typescript
// User.create({ email: 'teste@test.com', password: '12345' })

export { User }
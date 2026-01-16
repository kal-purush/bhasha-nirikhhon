import { Schema, Document, model } from 'mongoose';
import { compare, hash } from 'bcrypt';

import { signToken } from '../utils';

const SALT_FACTOR = 13;

const hashPassword = async (password: string): Promise<string> => {
  try {
    !password && null;
    return await hash(password, SALT_FACTOR);
  } catch (err) {
    console.log(err);
  }
};

type ItemPriority = 'LOW' | 'MEDIUM' | 'HIGH';

type PublicListCommentModel = Document & {
  posted_date: Date;
  contents: string;
  author: [UserModel];
};

/* <---------------- || ----------------> */
/* <---------------- || ----------------> */
/* <---------------- || ----------------> */

type ItemModel = Document & {
  priority: ItemPriority;
  starred: boolean;
  content: string;
  checked: boolean;
  comments: [PublicListCommentModel];
  complete_date: Date;
  checked_date: Date;
  reminder: Date;
};

const itemSchema = new Schema(
  {
    priority: { type: String, enum: ['LOW', 'MEDIUM', 'HIGH'] },
    starred: { type: Boolean, default: false },
    checked: { type: Boolean, default: false },
    content: String,
    comments: Array,
    complete_date: Date,
    checked_date: Date,
    reminder: [Date]
  },
  { timestamps: true }
);

/* <---------------- || ----------------> */
/* <---------------- || ----------------> */
/* <---------------- || ----------------> */

type ListModel = Document & {
  items: [ItemModel];
  name: string;
  private: boolean;
};

const listSchema = new Schema(
  {
    items: [itemSchema],
    name: String,
    private: Boolean
  },
  { timestamps: true }
);

/* <---------------- || ----------------> */
/* <---------------- || ----------------> */
/* <---------------- || ----------------> */

type UserModel = Document & {
  email: string;
  username: string;
  password: string;
  lists: [ListModel];
  accountCreated: Date;
  roommates: [UserModel];
  checkPassword: (password: string) => Promise<Boolean>;
  createToken: (id: string) => string;
};

const userSchema = new Schema(
  {
    lists: [listSchema],
    roommates: [this],
    email: { type: String, required: true, unique: true },
    firstName: String,
    lastName: String,
    password: { type: String, required: true },
    username: { type: String, required: true, unique: true }
  },
  { timestamps: true }
);

userSchema.pre('save', async function(this: UserModel, next) {
  try {
    /* if the user's password isn't modified, don't re-hash the password */
    !this.isModified('password') && next();

    /* hash that MF thang */
    this.password = await hashPassword(this.password);

    next();
  } catch (err) {
    next(err);
  }
});

userSchema.methods.checkPassword = async function(password: string): Promise<Boolean> {
  try {
    return await compare(password, this.password);
  } catch (error) {
    console.log(error);
  }
};

userSchema.methods.createToken = signToken;

const userModel = model<UserModel>('User', userSchema);
const listModel = model<ListModel>('List', listSchema);
const itemModel = model<ItemModel>('Item', itemSchema);

export { userModel, listModel, itemModel };
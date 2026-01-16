// user read (query one user), update and delete account 
import e, { Request, Response } from "express";
import Post from "../models/Post";

// GET posts by id
const getPostById = async (req: Request, res: Response) => {
    try {
        const id: string = String(req.query.id);
        // query the user with the email
        const foundPost = await Post.findOne({
            where: { id: id },
        });

        // if user is not found in the database
        if(!foundPost) {
            return res.status(404).json("Cannot find user with the provided email");
        }

        return res.status(200).json(foundPost);

    } catch(err) {
        console.error(err);
        return res.status(500).json({ error: err.message });
    }
}
// GET posts by userId
const getPostByUserId = async (req: Request, res: Response) => {
    try {
        const userId: string = String(req.query.userId);
        const postCount: number = parseInt(req.query.postCount as string);

        // Check if postCount is a valid positive integer
        if (isNaN(postCount) || postCount <= 0) {
            return res.status(400).json({ error: "Invalid postCount value. It should be a positive integer." });
        }

        const foundPosts = await Post.findAll({
            where: { creatorId: userId },
            limit: postCount, // Limit the number of posts returned based on postCount
        });

        // if user is not found in the database
        if (!foundPosts || foundPosts.length === 0) {
            return res.status(404).json({ error: "Cannot find posts for the provided userId." });
        }

        return res.status(200).json(foundPosts);

    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: err.message });
    }
}

// POST create new post
const updatePassword = async (req: Request, res: Response) => {
    try {
        // get the cookies from client
        const authToken = req.cookies['authToken'];

        // decode the token and get the user's email and the password the user's want to update
        const decodedToken : any = jwt.verify(authToken, process.env.JWT_SECRET_KEY);
        const email: string = decodedToken.email;
        const update_password: string = req.body["update_password"];
        
        // query the user's to update the user's password
        const findUser = await User.findOne({
            where: {email : email},
        })

        if(!findUser) {
            return res.status(404).json({ message: `User doesnt exists`});
        }
        
        // update the user's password
        // Hash the new password
        const salt = await bcrypt.genSalt();
        const update_hashPassword = await bcrypt.hash(update_password, salt);

        // Is new password cannot be the same with old password?????????
        findUser.password = update_hashPassword;

        await findUser.save();

        return res.status(201).json({ message: `Update password successfully!` });

    } catch(err) {
        console.log(err);
        return res.status(500).json({ error: err.message });
    }
}

const updateUser = async (req: Request, res: Response) => {
    try {
        // get the cookies from front-end
        const authToken = req.cookies['authToken'];
        const fieldsToUpdate = req?.body?.fieldsToUpdate;

        // decode the token and get the user's email for query
        const decodedToken: any = jwt.verify(authToken, process.env.JWT_SECRET_KEY);
        const email: string = decodedToken.email;
        // query the user with the email
        let foundUser = await User.findOne({
            where: { email: email },
        });

        // if user is not found in the database
        if(!foundUser) {
            return res.status(404).json("Cannot find user with the provided email");
        }
        
        Object.entries(fieldsToUpdate).forEach(([key, value]) => {
            if (key in foundUser) {
                // @ts-ignore
                foundUser[key] = value;
            }
        });

        foundUser.save();
        


        // response 200 if query successful
        return res.status(200).json(foundUser);

    } catch(err) {
        console.error(err);
        return res.status(500).json({ error: err.message });
    }
}

// DELETE request to delete the user's account
const deleteAccount = async (req:Request, res:Response) => {
    // get the user's email from cookies
    try {
        const authToken = req.cookies['authToken'];

        // decode the token and get the user's email to delete the account
        const decodedToken : any = jwt.verify(authToken, process.env.JWT_SECRET_KEY);
        const email: string = decodedToken.email;
        
        // query to get the user's to delete the account
        const findUser = await User.findOne({
            where: {email : email},
        })

        if(!findUser) {
            return res.status(404).json({ message: `User doesnt exists`});
        }

        // delete the user's account
        await findUser.destroy();
        return res.status(201).json({ message: `User account deleted successfully`});
    } catch(err) {
        console.log(err);
        return res.status(500).json({ message: err.message });
    }
}

export { getUser, logOutUser, updatePassword, deleteAccount, updateUser };
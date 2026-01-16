import { z } from 'zod';
import Admin from '../../database/models/Admin.js';
import User from '../../database/models/User.js';
import {
    errorResponse,
    sendSuccessResponse,
} from '../../utilities/Responses.js';
import catchAsync from '../../utilities/catchAsync.js';
import { comparePassword, hashPassword } from '../../utilities/hashPassword.js';
import { getAccessorId } from '../../utilities/utilitis.js';
const schema = z.object({
    newPassword: z
        .string({
            required_error: 'New Password is required',
        })
        .trim()
        .min(1, { message: 'New  Password is required' })
        .refine(
            (data) =>
                /^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[!@#$%^&*_=+-]).{8,16}$/.test(
                    data,
                ),
            {
                message:
                    'New  Password Must have Lowercase, Uppercase, Number, Symbol or special char',
            },
        ),
    oldPassword: z
        .string({
            required_error: 'Old Password is required',
        })
        .trim()
        .min(1, { message: 'Old password is required' })
        .refine(
            (data) =>
                /^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[!@#$%^&*_=+-]).{8,16}$/.test(
                    data,
                ),
            {
                message:
                    'Old Password Must have Lowercase, Uppercase, Number, Symbol or special char',
            },
        ),
});
const adminChangePassword = catchAsync(async (req, res) => {
    schema.parse(req.body);
    const { newPassword, oldPassword } = req.body;

    const adminId = getAccessorId(req);

    const isUserExists = await Admin.findOne({
        _id: adminId,
    });
    if (!isUserExists) {
        return res.status(401).json({
            success: false,
            message: 'You are not Authorized',
        });
    }

    const isMatched = await comparePassword(oldPassword, isUserExists.password);

    if (!isMatched) {
        return res.status(400).json(
            errorResponse({
                message: 'wrong old password',
            }),
        );
    }

    const hashedPassword = await hashPassword(newPassword);

    const updatedUser = await Admin.findOneAndUpdate(
        { _id: adminId },
        { $set: { password: hashedPassword } },
        { new: true },
    )
      

    return sendSuccessResponse({
        res,
        message: 'Password Updated Successfully',
        data: updatedUser,
    });
});
export default adminChangePassword;
//# sourceMappingURL=resetPassword.js.map
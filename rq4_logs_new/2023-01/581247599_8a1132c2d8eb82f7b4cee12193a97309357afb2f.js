import Users from "../models/Users.js";

export const createUser = async (req, res) => {
    const {
        businessName,
        RFC,
        fristNameTitular,
        lastNameTitular,
        email,
        street,
        innerNumber,
        outdoorNumber,
        postalCode,
        suburb,
        city,
        state,
        officePhoneNumber,
        mobilePhoneNumber,
        totalEmployees,
        totalRFC,
        monthlyDebt,
        userAssigned,
        passwordAssigned
    } = req.body;
    const newUser = new Users({
        businessName,
        RFC,
        fristNameTitular,
        lastNameTitular,
        email,
        street,
        innerNumber,
        outdoorNumber,
        postalCode,
        suburb,
        city,
        state,
        officePhoneNumber,
        mobilePhoneNumber,
        totalEmployees,
        totalRFC,
        monthlyDebt,
        userAssigned,
        passwordAssigned
    });
    await newUser.save()
    return res.json(newUser)
};
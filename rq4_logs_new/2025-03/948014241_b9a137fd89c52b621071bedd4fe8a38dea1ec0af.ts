import { body, ValidationChain } from 'express-validator'


export const validateUpdatUserBySelf = (): ValidationChain[] => [

    body('userName')
        .trim()
        .notEmpty()
        .withMessage('Tên người dùng không được để trống')
        .isLength({ min: 3 })
        .withMessage('Tên người dùng phải có ít nhất 3 ký tự'),

    // body('email').trim().notEmpty().withMessage('Email không được để trống').isEmail().withMessage('Email không hợp lệ'),

    // body('phone')
    //     .trim()
    //     .notEmpty()
    //     .withMessage('Số điện thoại không được để trống')
    //     .matches(/^[0-9]{10,11}$/)
    //     .withMessage('Số điện thoại phải có 10-11 chữ số'),

    body('birthDate')
        .optional()
        .matches(/^(0[1-9]|[12][0-9]|3[01])-(0[1-9]|1[0-2])-\d{4}$/)
        .withMessage('Ngày sinh không hợp lệ, định dạng đúng là dd-mm-yyyy'),

    body('gender')
        .optional()
        .isBoolean()
        .withMessage('Giới tính phải là true hoặc false'),

    body('address')
        .optional()
        .isString()
        .withMessage('Địa chỉ không hợp lệ')

]

export const validateUserByAdmin = (): ValidationChain[] => [

    body('roleName')
        .trim()
        .notEmpty()
        .withMessage('Vai trò không được để trống')
        .isIn(['User', 'Doctor'])
        .withMessage('Vai trò phải là User hoặc Doctor'),
    body('userName')
        .trim()
        .notEmpty()
        .withMessage('Tên người dùng không được để trống')
        .isLength({ min: 3 })
        .withMessage('Tên người dùng phải có ít nhất 3 ký tự'),

    body('email')
        .trim()
        .notEmpty()
        .withMessage('Email không được để trống')
        .isEmail()
        .withMessage('Email không hợp lệ'),

    body('phone')
        .trim()
        .notEmpty()
        .withMessage('Số điện thoại không được để trống')
        .matches(/^[0-9]{10,11}$/)
        .withMessage('Số điện thoại phải có 10-11 chữ số'),

    body('birthDate')
        .notEmpty()
        .withMessage('Ngày sinh không được để trống')
        .matches(/^(0[1-9]|[12][0-9]|3[01])-(0[1-9]|1[0-2])-\d{4}$/)
        .withMessage('Ngày sinh không hợp lệ, định dạng đúng là dd-mm-yyyy'),

    body('gender')
        .notEmpty()
        .withMessage('Giới tính không được để trống')
        .isBoolean()
        .withMessage('Giới tính phải là true hoặc false'),

    body('address')
        .notEmpty()
        .withMessage('Địa chỉ không được để trống')
        .isString()
        .withMessage('Địa chỉ không hợp lệ')
]
const { type } = require("@hapi/joi/lib/extend");

const SERVER = {
    JWT_SECRET_KEY: "MaEHqzXzdWrCS6TS",
    Login: {
        subject: "Login Details",
        body: `<b>Welcome, {name}</b><p>Here's your login information:  </p>
        <p><b>email: </b>{email}</p>
        <p><b>password: </b>{password}</p>`,
    },
    otpEmail: {
        subject: "OTP Verification",
        body: `<p>Hi</p><p> OTP - <b>{otp}</b> </p>`,
    },
    RejectEmail: {
        subject: "Reject Verification",
        body: `<p>Hi</p><p>Here's Reject <b>{reason}</b></p>`,
    }
};

const DATABASE = {
    USER_TYPE: {
        ADMIN: "ADMIN",
        USER: "USER",
        VENDOR: "VENDOR"
    },
    OTP_TYPE: {
        EMAIL_VERIFICATION: "EMAIL_VERIFICATION",
        FORGOT_PASSWORD: "FORGOT_PASSWORD",
        PHONE_VERIFICATION: "PHONE_VERIFICATION",
        ADMIN_VERIFICATION: "ADMIN_VERIFICATION",
    },
    ORDER_STATUS: {
        DISPATCHED: 'DISPATCHED',
        UNSHIPPED: 'UNSHIPPED',
        PENDING: 'PENDING',
        COMPLETED: 'COMPLETED',
        CANCELLED: 'CANCELLED',
        FAILED: 'FAILED',
        REFUND: 'REFUND',
        EXCHANGE: 'EXCHANGE',
        RETURN: 'RETURN'
    },
    PAYMENT_MODE: {
        COD: 'COD',
        ONLINE: 'ONLINE'
    },
    PRODUCT_STATUS: {
        STOCK: "STOCK",
        OUTOFSTOCK: "OUTOFSTOCK",
    },
    COUPON_STATUS: {
        AVALIBLE: "AVALIBLE",
        NOTAVALIBLE: "NOTAVALIBLE"
    },
    DISCOUNT_TYPE: {
        PERCENTAGE: 'PERCENTAGE',
        AMOUNT: 'AMOUNT'
    },
    OFFER_STATUS: {
        ACTIVE: 'ACTIVE',
        EXPIRY: 'EXPIRY'
    },
    DISCOUNT_CODE_METHOD: {
        CODE: 'CODE',
        AUTOMATIC: 'AUTOMATIC'
    }
};

const STATUS_MSG = {
    ERROR: {
        IMP_ERROR: {
            statusCode: 500,
            customMessage: "Implementation error",
            type: "IMP_ERROR",
        },
        INVALID_PASSWORD: {
            statusCode: 400,
            customMessage: "Password you have entered does not match.",
            type: "INVALID_PASSWORD",
        },
        ADMIN_NOT_FOUND: {
            statusCode: 400,
            customMessage: "Admin not found",
            type: "ADMIN_NOT_FOUND",
        },
        USER_NOT_FOUND: {
            statusCode: 400,
            customMessage: "User not found",
            type: "User_NOT_FOUND",
        },
        INVALID_OTP: {
            statusCode: 400,
            customMessage: "The OTP you have entered does not match or expired",
            type: "INVALID_OTP",
        },
        NOT_EXIST: {
            statusCode: 400,
            customMessage: "The record does not exist",
            type: "NOT_EXIST",
        },
        CART_NOT_EXIT: {
            statusCode: 400,
            customMessage: "Cart Not Exit",
            type: "CART_NOT_EXIT",
        },
        CART_ALREAY_EXIT: {
            statusCode: 400,
            customMessage: "Cart Already Exit",
            type: "CART_ALREAY_EXIT",
        },
        PRODUCT_ALREAY_EXIT: {
            statusCode: 400,
            customMessage: "Product Already Exit",
            type: "PRODUCT_ALREAY_EXIT",
        },
        PHONE_ALREADY_EXIT: {
            statusCode: 400,
            customMessage: "Mobile Number Already Exit",
            type: "PHONE_ALREADY_EXIT"
        },
        EMAIL_ALREADY_EXIT: {
            statusCode: 400,
            customMessage: "username Already Resgister With Us",
            type: "EMAIL_ALREADY_EXIT"
        },
        VENDOR_NOT_EXIT: {
            statusCode: 400,
            customMessage: "Vendor Not Exit",
            type: "VENDOR_NOT_EXIT"
        },
        BRAND_ALREADY_EXIT: {
            statusCode: 400,
            customMessage: "Brand Already Exit",
            type: "BRAND_ALREADY_EXIT"
        },
        OFFER_ALREADY_EXIT: {
            statusCode: 400,
            customMessage: "Offer Already Exit",
            type: "OFFER_ALREADY_EXIT"
        },
        TOKEN_EXPIRED: {
            statusCode: 400,
            customMessage: "Token Expired",
            type: "TOKEN_EXPIRED"
        },
        BLOCKED: {
            statusCode: 400,
            customMessage: "This Account blocked By Admin",
            type: "BLOCKED"
        },
        UNBLOCK: {
            statusCode: 400,
            customMessage: "This Account Unblocked By Admin"
        },
        NOT_DISCOUNT: {
            statusCode: 400,
            customMessage: "You Dont Get Discount ",
            type: "NOT_DISCOUNT"
        },
        ALREADY_DISCOUNTED: {
            statusCode: 400,
            customMessage: "You Already Get Discount",
            type: "ALREADY_DISCOUNTED"
        },
        REVIEW_ALREADY_EXIT: {
            statusCode: 400,
            customMessage: "Review already Exit",
            type: "REVIEW_ALREADY_EXIT"
        },
        PRODUCT_ALREAY_EXIT_CART: {
            statusCode: 400,
            customMessage: "Product Already Exit In Cart",
            type: "PRODUCT_ALREADY_EXIT_CART"
        }
    },
    SUCCESS: {
        CREATED: {
            statusCode: 200,
            customMessage: "Craeted Successfully",
            type: "CREATED"
        },
        UPDATED: {
            statusCode: 200,
            customMessage: "Updated Successfully",
            type: "UPDATED",
        },
        DELETED: {
            statusCode: 200,
            customMessage: "Deleted Successfully",
            type: "DELETED",
        },
        ACTIVE: {
            statusCode: 200,
            customMessage: 'Active Successfully',
            type: 'Active'
        },
        INACTIVE: {
            statusCode: 200,
            customMessage: 'Inactive Successfully',
            type: 'Inactive'
        },
        ORDERS: {
            statusCode: 200,
            customMessage: 'Order created Successfully',
            type: 'Order'
        },
        BLOCK: {
            statusCode: 200,
            customMessage: "Block Successfully",
            type: "Product"
        },
        UNBLOCK: {
            statusCode: 200,
            customMessage: "Unblock Successfully",
            type: "Product"
        },
        PUBLISHED: {
            statusCode: 200,
            customMessage: "Publish Successfully",
            type: "PUBLISHED"
        },
        UNPUBLISHED: {
            statusCode: 200,
            customMessage: "Unpublish Successfully",
            type: "UNPUBLISHED"
        },
        VENDOR_BLOCK: {
            statusCode: 200,
            customMessage: "Vendor Blocked Successfully",
            type: "VENDOR_BLOCK"
        },
        DEFAULT: {
            statusCode: 200,
            customMessage: "Success",
            type: "DEFAULT"
        },
        DISCOUNT: {
            statusCode: 200,
            customMessage: "Discount Successfully",
            type: "DISCOUNT"
        },
        SUBADMIN_BLOCKED: {
            statusCode: 400,
            customMessage: "This Account Blocked By Admin",
            type: "SUBADMIN_BLOCKED"
        },
        SUBADMIN_UNBLOCKED: {
            statusCode: 400,
            customMessage: "This Account Un Blocked By Admin",
            type: "SUBADMIN_UNBLOCKED"
        }
    },

};

const swaggerDefaultResponseMessages = {
    200: { description: "Success" },
    400: { description: "Bad Request" },
    401: { description: "Unauthorized" },
    404: { description: "Data Not Found" },
    500: { description: "Internal Server Error" },
};


let APP_CONSTANTS = {
    SERVER: SERVER,
    DATABASE: DATABASE,
    STATUS_MSG: STATUS_MSG,
    swaggerDefaultResponseMessages: swaggerDefaultResponseMessages,
};

module.exports = APP_CONSTANTS;
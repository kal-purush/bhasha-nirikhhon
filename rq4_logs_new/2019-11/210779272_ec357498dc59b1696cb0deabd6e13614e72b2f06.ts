import { Schema } from 'express-validator';

import { mentorRoles } from '../../utils/type';

export function addressVal(base?: string): Schema {
    const path = base ? `${base}.` : '';
    return {
        [`${path}country`]: {
            in: ['body'],
            isString: { errorMessage: 'Country must be of type string' },
            exists: { errorMessage: 'Country must be defined' },
            trim: true,
            escape: true,
        },
        [`${path}city`]: {
            in: ['body'],
            isString: { errorMessage: 'City must be of type string' },
            exists: { errorMessage: 'City must be defined' },
            trim: true,
            escape: true,
        },
        [`${path}postalCode`]: {
            in: ['body'],
            isString: { errorMessage: 'Postal code must be of type string' },
            exists: { errorMessage: 'Postal code must be defined' },
            trim: true,
            escape: true,
        },
        [`${path}address`]: {
            in: ['body'],
            isString: { errorMessage: 'Address must be of type string' },
            exists: { errorMessage: 'Address must be defined' },
            trim: true,
            escape: true,
        },
        [`${path}additional`]: {
            in: ['body'],
            isString: { errorMessage: 'Additional must be of type string' },
            optional: true,
            trim: true,
            escape: true,
        },
    };
}

export function businessVal(base?: string): Schema {
    const path = base ? `${base}.` : '';
    return {
        [`${path}name`]: {
            in: ['body'],
            isString: { errorMessage: 'Name must be of type string' },
            exists: { errorMessage: 'Name must be defined' },
            trim: true,
            escape: true,
        },

        ...addressVal(base),
    };
}

export function campaignVal(base?: string): Schema {
    const path = base ? `${base}.` : '';
    return {
        [`${path}name`]: {
            in: ['body'],
            isString: { errorMessage: 'Name must be of type string' },
            exists: { errorMessage: 'Name must be defined' },
            trim: true,
            escape: true,
        },
        [`${path}description`]: {
            in: ['body'],
            isString: { errorMessage: 'Description must be of type string' },
            exists: { errorMessage: 'Description must be defined' },
            trim: true,
            escape: true,
        },
        [`${path}semester`]: {
            in: ['body'],
            isString: { errorMessage: 'Semester must be of type string' },
            exists: { errorMessage: 'Semester must be defined' },
            trim: true,
            escape: true,
        },
        [`${path}maxProposition`]: {
            in: ['body'],
            isInt: { errorMessage: 'maxProposition must be an integer >= 0', options: { min: 0 } },
            optional: true,
            toInt: true,
        },
        [`${path}isPublish`]: {
            in: ['body'],
            isBoolean: { errorMessage: 'Publish must be of type boolean' },
            optional: true,
            toBoolean: true,
        },
        [`${path}startAt`]: {
            in: ['body'],
            isInt: { errorMessage: 'Start At must be of type timestamp', options: { min: 0 } },
            optional: true,
            toInt: true,
        },
        [`${path}endAt`]: {
            in: ['body'],
            isInt: { errorMessage: 'End At must be of type timestamp', options: { min: 0 } },
            optional: true,
            toInt: true,
        },
    };
}

export function categoryVal(base?: string): Schema {
    const path = base ? `${base}.` : '';
    return {
        [`${path}label`]: {
            in: ['body'],
            isString: { errorMessage: 'Label must be of type string' },
            exists: { errorMessage: 'Label must be defined' },
            trim: true,
            escape: true,
        },
    };
}

export function fileVal(base?: string): Schema {
    const path = base ? `${base}.` : '';
    return {
        [`${path}name`]: {
            in: ['body'],
            isString: { errorMessage: 'Name must be of type string' },
            exists: { errorMessage: 'Name must be defined' },
            trim: true,
            escape: true,
        },
        [`${path}type`]: {
            in: ['body'],
            isString: { errorMessage: 'Type must be of type string' },
            exists: { errorMessage: 'Type must be defined' },
            trim: true,
            escape: true,
        },
    };
}

export function internshipVal(base?: string): Schema {
    const path = base ? `${base}.` : '';
    return {
        [`${path}subject`]: {
            in: ['body'],
            isString: { errorMessage: 'Subject must be of type string' },
            exists: { errorMessage: 'Subject must be defined' },
            trim: true,
            escape: true,
        },
        [`${path}description`]: {
            in: ['body'],
            isString: { errorMessage: 'Description must be of type string' },
            exists: { errorMessage: 'Description must be defined' },
            trim: true,
            escape: true,
        },

        ...addressVal(base),

        [`${path}isInternshipAbroad`]: {
            in: ['body'],
            isBoolean: { errorMessage: 'Internship abroad must be of type boolean' },
            optional: true,
            toBoolean: true,
        },
        [`${path}isValidated`]: {
            in: ['body'],
            isBoolean: { errorMessage: 'Validated must be of type boolean' },
            optional: true,
            toBoolean: true,
        },
        [`${path}isProposition`]: {
            in: ['body'],
            isBoolean: { errorMessage: 'Proposition must be of type boolean' },
            optional: true,
            toBoolean: true,
        },
        [`${path}isPublish`]: {
            in: ['body'],
            isBoolean: { errorMessage: 'Publish must be of type boolean' },
            optional: true,
            toBoolean: true,
        },

        [`${path}publishAt`]: {
            in: ['body'],
            isInt: { errorMessage: 'Publish at must be a timestamp', options: { min: 0 } },
            optional: true,
            toInt: true,
        },
        [`${path}startAt`]: {
            in: ['body'],
            isInt: { errorMessage: 'Start at must be a timestamp', options: { min: 0 } },
            optional: true,
            toInt: true,
        },
        [`${path}endAt`]: {
            in: ['body'],
            isInt: { errorMessage: 'End at must be a timestamp', options: { min: 0 } },
            optional: true,
            toInt: true,
        },
    };
}

export function studentVal(base?: string): Schema {
    const path = base ? `${base}.` : '';
    return {
        [`${path}firstName`]: {
            in: ['body'],
            isString: { errorMessage: 'First name must be of type string' },
            exists: { errorMessage: 'First name must be defined' },
            trim: true,
            escape: true,
        },
        [`${path}lastName`]: {
            in: ['body'],
            isString: { errorMessage: 'Last name must be of type string' },
            exists: { errorMessage: 'Last name must be defined' },
            trim: true,
            escape: true,
        },
        [`${path}email`]: {
            in: ['body'],
            isString: { errorMessage: 'Email must be of type string' },
            isEmail: { errorMessage: 'Email must complain to email struct' },
            exists: { errorMessage: 'Email must be defined' },
            trim: true,
            escape: true,
        },
        [`${path}semester`]: {
            in: ['body'],
            isString: { errorMessage: 'Semester must be of type string' },
            exists: { errorMessage: 'Semester must be defined' },
            trim: true,
            escape: true,
        },
    };
}

export function propositionsVal(base?: string): Schema {
    const path = base ? `${base}.` : '';
    return {
        [`${path}comment`]: {
            in: ['body'],
            isString: { errorMessage: 'Comment must be of type string' },
            exists: { errorMessage: 'Comment must be defined' },
            trim: true,
            escape: true,
        },
    };
}

export function mentorVal(base?: string): Schema {
    const path = base ? `${base}.` : '';
    return {
        [`${path}firstName`]: {
            in: ['body'],
            isString: { errorMessage: 'First name must be of type string' },
            exists: { errorMessage: 'First name must be defined' },
            trim: true,
            escape: true,
        },
        [`${path}lastName`]: {
            in: ['body'],
            isString: { errorMessage: 'Last name must be of type string' },
            exists: { errorMessage: 'Last name must be defined' },
            trim: true,
            escape: true,
        },
        [`${path}email`]: {
            in: ['body'],
            isString: { errorMessage: 'Email must be of type string' },
            isEmail: { errorMessage: 'Email must complain to email struct' },
            exists: { errorMessage: 'Email must be defined' },
            trim: true,
            escape: true,
        },
        [`${path}role`]: {
            in: ['body'],
            isString: { errorMessage: 'Role must be of type string' },
            isIn: {
                options: [mentorRoles],
                errorMessage: `Role must be in [${mentorRoles.join(', ')}]`,
            },
            optional: true,
            trim: true,
        },
    };
}
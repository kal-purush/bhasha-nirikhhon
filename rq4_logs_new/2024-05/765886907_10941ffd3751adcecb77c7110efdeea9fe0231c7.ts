import { GraphQLScalarType, Kind } from 'graphql';

export const DMSP_BASE_URL: string = process.env.DMSP_BASE_URL;
const DMSP_REGEX: RegExp = /^[0-9a-zA-Z]+$/;

export function validateDmspId(val) {
  const id = val.split(DMSP_BASE_URL)[1];
  if (val.startsWith(DMSP_BASE_URL) && id.match(DMSP_REGEX).length > 0) {
    return val;
  }
  throw new Error(`Invalid DMSP ID. Expected: "${DMSP_BASE_URL}A1B2C3D4"`);
};

// GraphQL Scalar definition for a DMSP ID
export const dmspIdScalar = new GraphQLScalarType({
  name: 'DmspId',
  description: 'A Data Management and Sharing Plan\'s (DMSP) ID',

  serialize(dmspId) {
    return validateDmspId(dmspId.toString());
  },

  parseValue(value) {
    return validateDmspId(value.toString());
  },

  parseLiteral(ast) {
    if (ast.kind === Kind.STRING) {
      // Convert hard-coded AST string to string and then to DmspId
      return validateDmspId(ast.value.toString());
    }
    // Invalid hard-coded value (not a string)
    return null;
  },
});
import { GraphQLScalarType, Kind } from 'graphql';

// An organization DMSP ID
export class DmspId {
  value: string;

  static baseURL: string = process.env.DMSP_BASE_URL;
  static idRegex: RegExp = /^[0-9a-zA-Z]+$/;

  static validFormats: string = `"${DmspId.baseURL}A1B2C3D4"`;

  constructor(val: string) {
    this.value = this.validate(val);
  };

  validate(val) {
    const id = val.split(DmspId.baseURL)[1];
    if (val.startsWith(DmspId.baseURL) && id.match(DmspId.idRegex).length > 0) {
      return val;
    }
    throw new Error(`Invalid DMSP ID. Expected: ${DmspId.validFormats}`);
  };
};

// GraphQL Scalar definition for a DMSP ID
export const dmspIdScalar = new GraphQLScalarType({
  name: 'DmspId',
  description: 'A Data Management and Sharing Plan\'s (DMSP) ID',

  serialize(dmspId) {
    if (dmspId instanceof DmspId) {
      return dmspId.value; // Convert outgoing DMSP ID to a String for JSON
    }
    throw Error('GraphQL DmspId Scalar serializer expected an `DmspId` object');
  },

  parseValue(value) {
    if (typeof value === 'string') {
      return new DmspId(value); // Convert incoming string to DmspId
    }
    throw new Error(`GraphQL DmspId Scalar expected a string in the ${DmspId.validFormats} formats`);
  },

  parseLiteral(ast) {
    if (ast.kind === Kind.STRING) {
      // Convert hard-coded AST string to string and then to DmspId
      return new DmspId(ast.value.toString());
    }
    // Invalid hard-coded value (not a string)
    return null;
  },
});
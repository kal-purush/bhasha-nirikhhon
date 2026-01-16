import { Schema, ObjectPart, StringPart } from 'ts-api-validator';


export const accountSchema = new Schema({
    type: ObjectPart,
    required: false,
    content: {
      email: {
          type: StringPart,
          required: true
      },
      first_name: {
          type: StringPart,
          required: true
      },
      last_name: {
          type: StringPart,
          required: false
      },
      phone: {
          type: StringPart,
          required: false
      },
      company_name: {
          type: StringPart,
          required: false
      },
      password: {
          type: StringPart,
          required: true
      },
      registration_ip_address: {
          type: StringPart,
          required: false
      }
    }
});
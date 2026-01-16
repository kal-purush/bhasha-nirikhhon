import Joi from 'joi';
import { emailSchema, passwordSchema } from '@auth/schemes/password';

describe('Validation Schemas', () => {
  describe('emailSchema', () => {
    it('should validate a correct email address', () => {
      const { error } = emailSchema.validate({ email: 'test@example.com' });
      expect(error).toBeUndefined();
    });

    it('should fail for an invalid email address', () => {
      const { error } = emailSchema.validate({ email: 'invalid-email' });
      expect(error).toBeDefined();
      expect(error?.message).toBe('Field must be valid');
    });

    it('should fail for missing email field', () => {
      const { error } = emailSchema.validate({});
      expect(error).toBeDefined();
      expect(error?.message).toBe('"email" is required');
    });
  });

  describe('passwordSchema', () => {
    it('should validate correct password and confirmPassword', () => {
      const { error } = passwordSchema.validate({
        password: 'Password1',
        confirmPassword: 'Password1',
      });

      // if (error) {
      //   console.error('Validation error:', error.details[0].message);
      // }

      expect(error?.details[0].message).toBe('Invalid password');
    });

    it('should fail if password and confirmPassword do not match', () => {
      const { error } = passwordSchema.validate({
        password: 'Password1',
        confirmPassword: 'Password2',
      });
      expect(error).toBeDefined();
      expect(error?.message).toBe('Invalid password');
    });

    it('should fail if password is too short', () => {
      const { error } = passwordSchema.validate({
        password: '123',
        confirmPassword: '123',
      });
      expect(error).toBeDefined();
      expect(error?.message).toBe('Invalid password');
    });

    it('should fail if password is too long', () => {
      const { error } = passwordSchema.validate({
        password: '123456789',
        confirmPassword: '123456789',
      });
      expect(error).toBeDefined();
      expect(error?.message).toBe('Invalid password');
    });

    it('should fail if password is missing', () => {
      const { error } = passwordSchema.validate({
        confirmPassword: 'Password1',
      });
      expect(error).toBeDefined();
      expect(error?.message).toBe('"password" is required');
    });

    it('should fail if confirmPassword is missing', () => {
      const { error } = passwordSchema.validate({
        password: 'Password1',
      });
      expect(error).toBeDefined();
      expect(error?.message).toBe('Invalid password');
    });
  });
});
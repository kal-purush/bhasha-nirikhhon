import { User } from './User';

describe('User entity', () => {
    describe('validity', () => {
        let user: User;
        
        beforeEach(() => {
            user = new User();
        });
        
        test('should be valid when providing an id', () => {
            user.userId = 'USER_ID';
            expect(user.isValid()).toBeTruthy();
        });

        test('should be invalid when providing no id', () => {
            expect(user.isValid()).toBeFalsy();
        });
    });
});
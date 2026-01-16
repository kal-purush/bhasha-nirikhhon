import {AuthProvider} from './auth';
import { AngularFireAuth } from 'angularfire2/auth';

describe('Service: Auth', () => {

    let service: AuthProvider;
    let AFauth: AngularFireAuth;

    beforeEach(() => {
        service = new AuthProvider(AFauth);
    });

    it('should return true from loginUser when there is a valid user', () => {
        //localStorage.setItem('token', '1234'); 
        expect(service.loginUser("y@y.co", "123456")).toBeTruthy();
    });
    it('should return false from isAuthenticated when there is no token', () => {
        expect(service.loginUser("mauricio@gmail.com", "123456")).toBeFalsy();
    });


});
import { Observable, of } from "rxjs";
import { CognitoUser } from "../domain/models/cognito-user/cognito-user.model";
import { CognitoUserGateway } from "../domain/models/cognito-user/gateway/cognito-user.gateway";

export class MockCognitoUserGateway extends CognitoUserGateway  {
    override signUp(user: CognitoUser): Observable<any> {
        return of('success');
    }
    override confirmSignUp(user: CognitoUser): Observable<any> {
        return of('success');
    }
    override signIn(user: CognitoUser): Observable<any> {
        return of('success');
    }
    override signOut(): Observable<any> {
        return of('success');
    }
    override forgotPassword(email: string): Observable<any> {
        return of('success');
    }
    override forgotPasswordSubmit(email: string, code: string, password: string): Observable<any> {
        return of('success');
    }

}
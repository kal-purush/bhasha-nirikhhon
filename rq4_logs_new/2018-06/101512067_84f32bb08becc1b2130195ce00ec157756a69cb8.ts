
import { User, AccessRoles } from '@app/models';

import {
	AdminService, AuthService, CMSService, ContentService, HttpService, InterceptorService,
	IntersectionService, LoadingService, MobileService
} from '@app/services';


import { BehaviorSubject, of, Subject } from 'rxjs';



// ---------------------------------------
// ------------ AUTH SERVICE -------------
// ---------------------------------------

export const authServiceStub: Partial<AuthService> = {
	user: new BehaviorSubject(<User>{ username: null, role: null }),
	login: (user: User) => {
		throw Error('Implement in test');
	} // create in test
};
export type AuthServiceStub = typeof authServiceStub;




// ---------------------------------------
// ----------- MOBILE SERVICE ------------
// ---------------------------------------

const isMobile = new BehaviorSubject<boolean>(false);
export const mobileServiceStub: Partial<MobileService> = {
	isMobile: () => isMobile
};
export type MobileServiceStub = typeof mobileServiceStub;

/// <reference path='../../node_modules/inversify-dts/inversify/inversify.d.ts' />

import * as Inversify from 'inversify';
import { IBindingGroup } from '../binding-group.i';

import {
    IReviewService, ReviewService,
    IServiceManager, ServiceManager,
    IAuthenticationService, AuthenticationService,
    IUserService, UserService,
    INoodleService, NoodleService,
    IBrandService, BrandService
} from './_namespace';

export class BindingGroup implements IBindingGroup {

    public registerBindings(kernel: Inversify.interfaces.Kernel): void {
        kernel.bind<IServiceManager>('IServiceManager').to(ServiceManager);
        kernel.bind<IReviewService>('IReviewService').to(ReviewService);
        kernel.bind<IAuthenticationService>('IAuthenticationService').to(AuthenticationService);
        kernel.bind<IUserService>('IUserService').to(UserService);
        kernel.bind<INoodleService>('INoodleService').to(NoodleService);
        kernel.bind<IBrandService>('IBrandService').to(BrandService);
    }

}
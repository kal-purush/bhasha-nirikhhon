/// <reference path='../../node_modules/inversify-dts/inversify/inversify.d.ts' />

import * as Inversify from 'inversify';
import { IBindingGroup } from '../binding-group.i';

import {
    IUserSerializer, UserSerializer,
    IReviewSerializer, ReviewSerializer,
    INoodleSerializer, NoodleSerializer,
    IBrandSerializer, BrandSerializer
} from './_namespace';

export class BindingGroup implements IBindingGroup {

    public registerBindings(kernel: Inversify.interfaces.Kernel): void {
        kernel.bind<IUserSerializer>('IUserSerializer').to(UserSerializer);
        kernel.bind<IReviewSerializer>('IReviewSerializer').to(ReviewSerializer);
        kernel.bind<INoodleSerializer>('INoodleSerializer').to(NoodleSerializer);
        kernel.bind<IBrandSerializer>('IBrandSerializer').to(BrandSerializer);
    }

}
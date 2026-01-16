import { AngularFireModule } from '@angular/fire';
import { AngularFireStorageModule } from '@angular/fire/storage';
import { AngularFireAuthModule } from '@angular/fire/auth';
import { NgModule } from '@angular/core';

import { environment } from '../environments/environment';


@NgModule({
    imports: [
        AngularFireModule.initializeApp(environment.firebase),
    ],
    exports: [
        AngularFireStorageModule,
        AngularFireAuthModule,
    ]
})
export class AngularFirebaseModule {}
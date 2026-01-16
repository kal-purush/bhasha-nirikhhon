import { ApplicationConfig } from '@angular/core';
import { provideRouter } from '@angular/router';

import { routes } from './app.routes';
import { HTTP_INTERCEPTORS, provideHttpClient, withInterceptorsFromDi } from '@angular/common/http';

import { provideToastr } from 'ngx-toastr';
import { provideAnimations } from '@angular/platform-browser/animations';

import { UsuarioService } from './services/usuario.service';
import { TokenInterceptorService } from './services/token-interceptor.service';
import { FacebookModule } from 'ngx-facebook';

export const appConfig: ApplicationConfig = {
  providers: [
    provideRouter(routes),    
    UsuarioService,
    {
      provide: HTTP_INTERCEPTORS,
      useClass: TokenInterceptorService,
      multi: true
      },
      {provide: FacebookModule, useValue: FacebookModule.forRoot()},
      provideToastr(),
      provideAnimations(),
      provideHttpClient(withInterceptorsFromDi())
  ]
};
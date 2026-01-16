import { RouterModule } from '@angular/router';
import { NgModule } from '@angular/core';

// Components
import { HomeComponent } from './home.component';

// Routes
import { HomeRoutes as routes } from './home.routes';

@NgModule({
    declarations:[
        HomeComponent
    ],
    imports: [
        RouterModule.forChild(routes)
    ]
})

export class HomeModule {}
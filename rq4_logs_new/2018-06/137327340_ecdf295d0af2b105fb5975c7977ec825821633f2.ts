import { Routes } from '@angular/router';
import { HomeComponent } from './home/home.component';
import { DetailComponent } from './detail/detail.component';
import { PATH_HOME, PATH_DETAIL_ID, PATH_DETAIL_FORMATIONS, PATH_DETAIL_SKILLS } from './consts';
import { FormationsComponent } from './formations/formations.component';
import { ConnaissancesComponent } from './connaissances/connaissances.component';


export const ROUTES: Routes = [
    { path: PATH_HOME, component: HomeComponent },
    {
        path: PATH_DETAIL_ID, component: DetailComponent,
        children: [
            { path: PATH_DETAIL_FORMATIONS, component: FormationsComponent },
            { path: PATH_DETAIL_SKILLS, component: ConnaissancesComponent }
        ]
    }

];
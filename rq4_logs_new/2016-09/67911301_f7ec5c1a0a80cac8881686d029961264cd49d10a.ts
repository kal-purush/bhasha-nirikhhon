import{IntroductionComponent} from "./introduction/introduction.component";
import{CardGameComponent} from "./card-game/card-game.component";
import{JokesComponent} from "./jokes/jokes.component";
import{MovieComponent} from "./movie/movie.component";
import {provideRouter} from "@angular/router";
const APP_ROUTES=[
{path:'movie',component: MovieComponent},
{path:'',component: IntroductionComponent},
{path:'card-game',component: CardGameComponent},
{path:'joke',component: JokesComponent},


];
export const APP_ROUTES_PROVIDER=[
provideRouter(APP_ROUTES)

];
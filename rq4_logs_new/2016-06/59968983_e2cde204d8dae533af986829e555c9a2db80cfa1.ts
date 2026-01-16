import { Component } from "@angular/core";

import { HeroesComponent } from "../hero/hero.component";
import { HeroService } from "../../services/hero.service";

@Component({
    selector: "my-app",
    template: `
<h1>{{title}}</h1>
<nav>
    <a linkTo="/dashboard">Dashboard</a>
    <a linkTo="/heroes">Heroes</a>
</nav>
<route-view></route-view>
    `,
    directives: [
        HeroesComponent
    ],
    providers: [
        HeroService
    ]
})

export class AppComponent {
    title = "My Hero Academia";
}
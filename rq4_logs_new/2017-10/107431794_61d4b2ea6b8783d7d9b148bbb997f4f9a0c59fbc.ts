
import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { RankingComponent } from './components/ranking/ranking.component';

import { PlayerProfileComponent } from './components/player-profile/player-profile.component';

const routes: Routes = [
    {
        path: '',
        component: RankingComponent
    },
    {
        path: 'player-profile',
        component: PlayerProfileComponent
    }
    /*
    {
        path: 'scoreboard',
        component: ScoreboardComponent
    },
    {
        path: 'submit',
        component: SubmissionPageComponent
    },
    {
        path: 'discard-confirmation',
        component: DiscardConfirmationComponent
    }
    */
];

@NgModule({
    imports: [RouterModule.forRoot(routes, {useHash: true})],
    exports: [RouterModule]
})
export class AppRoutingModule { }
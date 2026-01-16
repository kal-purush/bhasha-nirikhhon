/// <reference path="../typings/angular2-meteor.d.ts" />
import {Component, View, bootstrap, NgFor} from 'angular2/angular2';

@Component({
    selector: 'app'
})
@View({
    templateUrl: 'client/app.html',
    directives: [NgFor]
})
class Management {
    projects: Object[] = [];
    constructor() {
        this.projects = [ 
            {
                name: "Project 1",
                description: "Lorem ipsum Dolor sint Duis. "
            },
            {
                name: "Project 2",
                description: "Shrine weathered meta-neon nano-dome render-farm dead euro-pop youtube RAF pistol shoes tower human car pen. Hotdog corrupted sunglasses singularity tank-traps corporation tower engine. Vinyl San Francisco neon long-chain hydrocarbons woman BASE jump convenience store computer. Sunglasses augmented reality film boy hotdog fluidity range-rover franchise claymore mine shoes Kowloon human grenade. Sentient shrine nodal point saturation point carbon systema market Tokyo systemic. Disposable man engine drone claymore mine Legba sensory dolphin tank-traps tattoo monofilament math-geodesic lights. Boy sunglasses meta-fluidity systema soul-delay warehouse Kowloon augmented reality neural sub-orbital media gang sprawl long-chain hydrocarbons receding. "
            },
            {
                name: "Project 3",
                description: "Cyber-A.I. monofilament tank-traps katana physical into. Numinous BASE jump augmented reality construct lights nano-girl assault man tiger-team cartel narrative military-grade. Smart-pistol cartel urban denim sentient footage fluidity saturation point rifle. Realism wristwatch claymore mine 3D-printed courier city human bridge carbon market shoes. Order-flow corrupted-ware cartel sprawl boat marketing beef noodles sub-orbital claymore mine woman knife tiger-team corporation futurity. Carbon pistol San Francisco neon bomb post-assassin receding. Decay otaku construct realism hacker nodality table. "
            },
            {
                name: "Project 4",
                description: "Alcohol pen BASE jump shrine courier cartel cyber-tanto girl. Weathered 3D-printed dissident computer assassin sign car disposable cyber-girl nano-tank-traps numinous artisanal spook Chiba. Motion human urban shrine sign youtube papier-mache wonton soup math-man neural rain cartel stimulate lights. Dome otaku shoes euro-pop towards spook math-warehouse tank-traps nodal point urban-space drone engine modem hacker. Augmented reality digital computer rifle Kowloon dissident j-pop wonton soup nano-skyscraper. "
            }
        ]
    }
}

bootstrap(Management);
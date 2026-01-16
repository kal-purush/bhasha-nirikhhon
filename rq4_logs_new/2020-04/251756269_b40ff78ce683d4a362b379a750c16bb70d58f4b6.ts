// title: string;
// description: string;
// requirements: Array<any>;
// heroes: Hero[];

import { Mission } from './models/mission';

//1, 2, or 3 heroes

export let TEST_MISSIONS: Mission[] = [
    {
        title: "Title",
        description: "this is a description of a test mission",
        requirements: ["requirements"],
        heroes: []
    },
    
    {
        title: "Title 2",
        description: "save gotham city lol",
        requirements: ["speed: 100"],
        heroes: []
    },

    {

        title: "Title 3",
        description: "save planet earth from an asteroid",
        requirements: ["strength: 9000, intelligence: 15"],
        heroes: []
    }
]
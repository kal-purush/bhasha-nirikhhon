class Overworld_Map {
    constructor(config) {
        // take a gameObject reference  
        this.gameObjects = config.gameObjects;

        this.lower_Img_Tag =  new Image(); // building <img/>
        this.lower_Img_Tag.src = config.lowerImg_Src // lower maps like tiles/Floor

        this.upper_Img_Tag =  new Image();
        this.upper_Img_Tag.src = config.upperImg_Src; // eveything above the characters : ex : roofd
    }

    drawLowerImage(ctx) {
        // drawing our the this.lowerSrcImg into the context defined in the Overworld class in Overworld.js
        ctx.drawImage(this.lower_Img_Tag, 0, 0)
    }

    drawUpperImage(ctx) {
        ctx.drawImage(this.upper_Img_Tag, 0, 0)
    }
}

// Defining some rooms and there content content to check the behavior of instanciated maps

window.Overword_Maps_Container = {
    DemoRoom : {
        lowerImg_Src: '/images/maps/DemoLower.png',
        upperImg_Src: '/images/maps/DemoUpper.png',
        gameObjects: {
            hero: new GameObject({
                x: 5,
                y: 6,
                src: '/images/characters/people/hero.png',
                shadowNeeded: true,
            }),
            npc1: new GameObject({
                x: 7,
                y: 9,
                src: '/images/characters/people/npc1.png',
                shadowNeeded: true,
            })
        }
    },
    Kitchen : {

        lowerImg_Src: '/images/maps/KitchenLower.png',
        upperImg_Src: '/images/maps/KitchenUpper.png',
        gameObjects: {
            hero: new GameObject({
                x: 3,
                y: 7,
                src: '/images/characters/people/hero.png',
                shadowNeeded: true,
            }),
            npc2: new GameObject({
                x: 5,
                y: 6,
                src: '/images/characters/people/npc2.png',
                shadowNeeded: true,
            }),
            npc3: new GameObject({
                x: 7,
                y: 9,
                src: '/images/characters/people/npc3.png',
                shadowNeeded: true,
            })
        }
    },
    // DinerRoom: {

    // },
}
///<amd-dependency path="angular-material" />

import Arts = require('./../arts/Arts');
import Component = require("./../setup/Component");

interface IScope extends Arts.IScope<IController> {

}

interface IController extends Arts.IController<IScope> {

}



class VocabularyController extends Arts.BaseController<IScope> implements IController {

    static $inject:string[] = ['$scope'];

    private baseURL:string;

    words : string[];



    constructor(public $scope:IScope) {
        super($scope);

        var component:Arts.IApplication = <Arts.IApplication>Arts.Arts.getModule(Component.NAME);
        this.baseURL = component.getBaseURL();
        console.log("ctrl loaded");
        this.words = [ 'ant.png',
            'ball.png',
            'baloon.png',
            'banana.png',
            'barber-shop.png',
            'basket.png',
            'beach.png',
            'beer.png',
            'bench.png',
            'boat.png',
            'book.png',
            'bottle.png',
            'bowl.png',
            'box.png',
            'bride.png',
            'broom.png',
            'butterfly.png',
            'cake.png',
            'camel.png',
            'car.png',
            'carrot.png',
            'cart.png',
            'cat.png',
            'chariot.png',
            'chicken.png',
            'clock.png',
            'closet.png',
            'cloud.png',
            'cow.png',
            'dancer.png',
            'detective.png',
            'doctor.png',
            'dog-female.png',
            'dog-small.png',
            'donkey.png',
            'driver.png',
            'duck.png',
            'dwarf.png',
            'elephant.png',
            'eraser.png',
            'family.png',
            'father.png',
            'fire.png',
            'fish.png',
            'five.png',
            'flag.png',
            'flower.png',
            'garden.png',
            'gate.png',
            'gift.png',
            'grandfather.png',
            'grapes.png',
            'greenhouse.png',
            'hala-bread.png',
            'hands.png',
            'hargol.png',
            'honey.png',
            'horse.png',
            'house.png',
            'ice-cream.png',
            'jam.png',
            'key.png',
            'kids.png',
            'lake.png',
            'lash.png',
            'lettuce.png',
            'light.png',
            'lion-small.png',
            'man.png',
            'Mikhol.png',
            'milk.png',
            'mountain.png',
            'mouse.png',
            'nadneda.png',
            'necklace.png',
            'night.png',
            'package.png',
            'palace.png',
            'pencil.png',
            'pillow.png',
            'plane.png',
            'police.png',
            'pot.png',
            'queue.png',
            'raashan.png',
            'rabbit.png',
            'ring.png',
            'road.png',
            'roof.png',
            'sailor.png',
            'shabat.png',
            'shoes.png',
            'singer.png',
            'singing-female.png',
            'six.png',
            'slide.png',
            'snake.png',
            'spider.png',
            'spoon.png',
            'stage.png',
            'strawberrie.png',
            'surfer.png',
            'table.png',
            'three.png',
            'tongue.png',
            'tractor.png',
            'traffic-light.png',
            'train.png',
            'turtle.png',
            'volcano.png',
            'wave.png',
            'wedding.png',
            'whell.png',
            'window.png',
            'woman.png' ];
    }

    cardClicked():void {

    }

}

export = VocabularyController;
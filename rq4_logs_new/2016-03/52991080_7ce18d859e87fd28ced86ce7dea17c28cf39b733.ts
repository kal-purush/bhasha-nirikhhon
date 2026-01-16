import { Component } from "../utilities/component-decorators";

@Component({
    selector: "gallery",
    templateUrl: "src/components/gallery.component.html",
    providers: ["$element"]
})
export class GalleryComponent {
    constructor(private $element: angular.IAugmentedJQuery) { }


    //no previous

    //if you click next you are on the last slide, display the invite to go to
    // the next gallery

    // if you are on the invite to the next gallery and you click next, go to the next gallery


    
} 
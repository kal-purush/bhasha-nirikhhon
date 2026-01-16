import { CanActivate, Component } from "../../../libs/component-decorators";
import { BrandActionCreator } from "../../actions";

@Component({
    route: "/brand/edit/:id",
    templateUrl: "wwwroot/components/brand/brand-editor.html",
    selector: "brand-editor",
    providers: ["$location", "brandActionCreator", "invokeAsync"]
})
@CanActivate(["$route", "invokeAsync", "brandActionCreator", ($route, invokeAsync, brandActionCreator: BrandActionCreator) => {
    var id = $route.current.params.id;
    return invokeAsync({
        action: brandActionCreator.getById,
        params: { id: id }
    });
}])
export class BrandEditorComponent {
    constructor(private $location, private brandActionCreator, private invokeAsync) { }

    storeOnChange = state => {
        this.id = null;
        this.name = null;
        this.photos = [];
        this.brandName = null;
    }

    addOrUpdate = () => {
        this.invokeAsync({
            action: this.brandActionCreator.addOrUpdate,
            params: {
                id: this.id,
                name: this.name,
                description: this.description
            }
        }).then(() => {
            if (!this.id && this.entities.filter(entity => entity.name === this.name).length > 0) {

            }
            else {

            }
        });
    }

    remove = () => this.brandActionCreator.remove({ id: this.id });

    id;
    name;
    photos: Array<any>;
    brandName;
    description;
    entities;
    baseUrl;
}
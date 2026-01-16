//https://angular.io/docs/ts/latest/tutorial/toh-pt1.html
import {bootstrap, Component, FORM_DIRECTIVES, CORE_DIRECTIVES} from 'angular2/angular2'
import {PouchDBService} from '../../service/pouchDbService.ts'
import {IStorageService} from '../../service/interface/storageService.ts'
import {Item} from '../../model/item'

@Component({
    selector: 'awesome-knowledge-app',
    directives: [FORM_DIRECTIVES, CORE_DIRECTIVES],
    templateUrl: 'app/components/awesome-knowledge-app/awesome-knowledge-app.view.html',
    styleUrls: ['app/components/awesome-knowledge-app/awesome-knowledge-app.style.css']
})
class AwesomeKnowledgeApp {
    private StorageService:IStorageService;

    public title = 'Awesome Knowledge';
    public items:Item[];
    public selectedItem:Item = null;

    constructor(StorageService:PouchDBService) {
        this.StorageService = StorageService;
        this.items = [];
        StorageService.findAll()
            .then(storedItems => {
                storedItems.forEach(i => {
                    this.items.push(i);
                });
            });
    }

    onSelect(item:Item) {
        this.selectedItem = item;
    }

    onClick() {
        this.items.push({id: this.items.length + 1, name: 'New awesome item', tags: []});
    }

    getSelectedClass(item:Item) {
        return {'selected': item === this.selectedItem};
    }
}

bootstrap(AwesomeKnowledgeApp, [PouchDBService]);
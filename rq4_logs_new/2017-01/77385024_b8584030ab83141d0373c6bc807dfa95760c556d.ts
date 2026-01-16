// import { InMemoryDbService } from 'angular2-in-memory-web-api';
import { InMemoryDbService } from '../../node_modules/angular2-in-memory-web-api';

export class InMemoryTeamsDataService implements InMemoryDbService {
  createDb() {
    let teams = [{
            id: '1',
            name: 'Directive3',
            form: 'onsite',
            maxUsers: '3',
            image_url: '',
            github: '',
            createdAd: '2016-12-30T16:12:13.840Z'
        },
        {
            id: '2',
            name: 'Directive31',
            form: 'onsite',
            maxUsers: '3',
            image_url: 'http://blog.ninja-squad.com/assets/images/ng2-ebook/ng2-logo.png',
            github: 'www.github.com/Directive31',
            createdAd: '2016-12-30T16:42:10.890Z'
        },
        {
            id: '3',
            name: 'SimpleLikeThat',
            form: 'onsite',
            maxUsers: '3',
            image_url: 'http://blog.ninja-squad.com/assets/images/ng2-ebook/ng2-logo.png',
            github: 'www.github.com/SimpleLikeThat',
            createdAd: '2016-12-31T10:42:10.890Z'
        }];
            return {teams};
    }
}
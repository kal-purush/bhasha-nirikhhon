import { Component } from 'angular2/core';

@Component({
    selector: 'kg-mainmenu',
    templateUrl: 'app/mainmenu/mainmenu.component.html',
    styleUrls: ['app/mainmenu/mainmenu.component.css']
})

export class MainmenuComponent {
    links: ILink[] = [
        { name: 'Home', url: 'home' },
        { name: 'News', url: 'news' },
        { name: 'Videos', url: 'videos' },
        { name: 'Teams & Players', url: 'teamsnplayers' },
        { name: 'About Konki', url: 'about' },
        { name: 'Contact', url: 'contact' },
    ];
    
}

interface ILink {
    name: string;
    url: string;
}
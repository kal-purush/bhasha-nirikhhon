import { Component, OnInit } from '@angular/core';

import 'style-loader!./search.scss';
import { ActivatedRoute, Router } from '@angular/router';
import { TitleChecker } from '../services';
import { BaMenuService } from '../../theme/services/baMenu/baMenu.service';

@Component({
    selector: 'search',
    templateUrl: './search.html'
})
export class Search implements OnInit {

    items: any = [];
    results: any = [];
    toSearch: string = '';

    constructor(protected _route: ActivatedRoute,
                protected _router: Router,
                protected _baMenuService: BaMenuService,
                protected _titleChecker: TitleChecker) {
    }

    ngOnInit() {
        this._route.params.subscribe(params => {
            this.toSearch = params['value'];
            this._titleChecker.setTitle('Search: "' + this.toSearch + '"');
            this.results = [];
            this.search();
        });
    }

    search(): any {
        this.prepareItems();
        this.items.map((item) => {
            if (item.title.toLowerCase().indexOf(this.toSearch.toLowerCase()) !== -1
            || (item.father && item.father.toLowerCase().indexOf(this.toSearch.toLowerCase()) !== -1)) {
                this.results.push(item);
            }
        });
    }

    prepareItems(): void {
        if (this.items.length <= 0) {
            this._baMenuService.menuItems.value.forEach((item) => {
                if (item.title && item.sidebar === true) {
                    if (item.children) {
                        item.children.forEach((child) => {
                            if (child.title && child.sidebar === true) {
                                this.items.push({
                                    father: item.title,
                                    title: child.title,
                                    path: child.route.path
                                });
                            }
                        });
                    } else {
                        this.items.push({
                            title: item.title,
                            path: item.route.path
                        });
                    }
                }
            });
        }

        console.log(this.items);
    }

    onResultClick(item: any): void {
        this._router.navigate(['pages/' + item.path]);
    }
}
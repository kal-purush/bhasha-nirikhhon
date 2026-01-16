import { Component, OnInit } from '@angular/core';
import { IPageLayout } from '../common';
import { ActivatedRoute } from '@angular/router';

@Component({
	moduleId: module.id,
	selector: 'app-page',
	templateUrl: 'page.component.html'
})
export class PageComponent implements OnInit {
	pageLayout: IPageLayout;

	constructor(private route: ActivatedRoute) {

	}

	ngOnInit(): void {
		this.route.data.forEach((data) => {
			if (data['pageLayout']) {
				this.pageLayout = data['pageLayout'];
			}
		});
	}
}
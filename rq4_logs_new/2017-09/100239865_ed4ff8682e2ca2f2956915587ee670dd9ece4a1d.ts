import { Component, OnInit } from '@angular/core';

@Component({
  selector: 'admin-category',
  templateUrl: './admin-category.component.html',
  styleUrls: ['./admin-category.component.scss']
})
export class AdminCategoryComponent implements OnInit { 
    newCategory = {
        active: true,
        name: '新增分类',
        customStyle: {
            'background': '#f7f7f7',
            'border-radius': '4px',
            'margin-bottom': '24px',
            'border': '0px'
        }
    };

    constructor() { }
    
    ngOnInit() { }
}
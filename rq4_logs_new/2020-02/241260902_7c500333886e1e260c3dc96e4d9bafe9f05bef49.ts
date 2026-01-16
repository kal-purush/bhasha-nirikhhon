import { Component, OnInit } from '@angular/core';

@Component({
  selector: 'app-categori',
  templateUrl: './categori.page.html',
  styleUrls: ['./categori.page.scss'],
})
export class CategoriPage implements OnInit {
  searchQuery: string = '';
  items: string[];
  list_ds:Array<any>=[
    {
      id:1,
      name:"thơi Trang Nam"
  },
  {
    id:2,
    name:"thơi Trang Nữ"
},
{
  id:3,
  name:"thơi Trang Trẻ Em"
},
{
  id:4,
  name:"thơi Trang Giày"
},
{
  id:5,
  name:"thơi Trang Nam"
}
];
  constructor() { }

  ngOnInit() {
  }

}
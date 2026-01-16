import {
  Component,
  EventEmitter,
  Input,
  Output
} from '@angular/core';


@Component({
  selector: 'app-card-item',
  templateUrl: './card-item.component.html',
  styleUrls: ['./card-item.component.scss']
})

export class CardItemComponent {

  @Input('row1') public row1: string;
  @Input('row2') public row2: string;
  @Input('images') public images: string;
  @Input('titleView') public titleView: string;
  @Input('titleDelete') public titleDelete: string;
  @Input('iconView') public iconView: string;
  @Input('iconDelete') public iconDelete: string;
  @Input('size') size: number;
  @Input('typeImages') typeImages = 'avatar';
  @Input('useDefault') useDefault = false;
  @Output('onDelete') public onDelete = new EventEmitter();
  @Output('onView') public onView = new EventEmitter();
  @Output('onSelect') public onSelect = new EventEmitter();

  public view() {
    this.onView.emit();
  }

  public delete() {
    this.onDelete.emit();
  }

}

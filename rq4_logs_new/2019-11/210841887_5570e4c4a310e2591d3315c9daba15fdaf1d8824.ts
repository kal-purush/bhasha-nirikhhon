import { Component, HostBinding, OnInit, ViewChild } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { AttributesService } from '@perun-web-apps/perun/services';
import { NotificatorService } from '../../../../../core/services/common/notificator.service';
import { MatDialog } from '@angular/material';
import { TranslateService } from '@ngx-translate/core';
import { AttributesListComponent } from '../../../attributes-list/attributes-list.component';
import { SelectionModel } from '@angular/cdk/collections';
import { Attribute } from '@perun-web-apps/perun/models';
import { CreateAttributeDialogComponent } from '../../../dialogs/create-attribute-dialog/create-attribute-dialog.component';
import { filterCoreAttributes } from '@perun-web-apps/perun/utils';
import { DeleteAttributeDialogComponent } from '../../../dialogs/delete-attribute-dialog/delete-attribute-dialog.component';
import { StoreService } from '../../../../../core/services/common/store.service';

@Component({
  selector: 'app-user-settings-attributes',
  templateUrl: './user-settings-attributes.component.html',
  styleUrls: ['./user-settings-attributes.component.scss']
})
export class UserSettingsAttributesComponent implements OnInit {

  @HostBinding('class.router-component') true;

  constructor(
    private route: ActivatedRoute,
    private attributesService: AttributesService,
    private notificator: NotificatorService,
    private dialog: MatDialog,
    private translate: TranslateService,
    private store: StoreService
  ) {
    this.translate.get('USER_DETAIL.SETTINGS.ATTRIBUTES.SUCCESS_SAVE').subscribe(value => this.saveSuccessMessage = value);
    this.translate.get('USER_DETAIL.SETTINGS.ATTRIBUTES.SUCCESS_DELETE').subscribe(value => this.deleteSuccessMessage = value);
  }

  @ViewChild('list', {static: false})
  list: AttributesListComponent;

  saveSuccessMessage: string;
  deleteSuccessMessage: string;
  selection = new SelectionModel<Attribute>(true, []);
  attributes: Attribute[] = [];
  userId: number;

  loading: boolean;
  filterValue = '';

  ngOnInit() {
    this.route.parent.parent.params.subscribe(params => {
      this.userId = params['userId'];
      if (this.userId === undefined) {
        this.userId = this.store.getPerunPrincipal().userId;
      }

      this.refreshTable();
    });
  }

  onCreate() {
    const dialogRef = this.dialog.open(CreateAttributeDialogComponent, {
      width: '1050px',
      data: {
        entityId: this.userId,
        entity: 'user',
        notEmptyAttributes: this.attributes,
        style: 'user-theme'
      }
    });

    dialogRef.afterClosed().subscribe(result => {
      if (result) {
        this.refreshTable();
      }
    });
  }

  onSave() {
    // have to use this to update attribute with map in it, before saving it
    this.list.updateMapAttributes();
    this.attributesService.setAttributes(this.userId, 'user', this.selection.selected).subscribe(() => {
      this.attributesService.getAllAttributes(this.userId, 'user').subscribe(attributes => {
        this.attributes = filterCoreAttributes(attributes);
        this.notificator.showSuccess(this.saveSuccessMessage);
        this.selection.clear();
      });
    });
  }

  onDelete() {
    const dialogRef = this.dialog.open(DeleteAttributeDialogComponent, {
      width: '450px',
      data: {
        entityId: this.userId,
        entity: 'user',
        attributes: this.selection.selected
      }
    });

    dialogRef.afterClosed().subscribe(didConfirm => {
      if (didConfirm) {
        this.refreshTable();
      }
    });
  }

  refreshTable() {
    // TODO Does not apply filter on refresh.
    this.loading = true;
    this.attributesService.getAllAttributes(this.userId, 'user').subscribe(attributes => {
      this.attributes = filterCoreAttributes(attributes);
      this.selection.clear();
      this.loading = false;
    });
  }

  applyFilter(filterValue: string) {
    this.filterValue = filterValue;
  }
}
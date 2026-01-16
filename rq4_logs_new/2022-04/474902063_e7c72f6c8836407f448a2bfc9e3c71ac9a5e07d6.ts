import { Component, OnInit, TemplateRef, ViewChild, ViewContainerRef } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { TranslateService } from '@ngx-translate/core';
import { IPepGenericListActions, IPepGenericListDataSource, PepGenericListService } from '@pepperi-addons/ngx-composite-lib/generic-list';
import { PepDialogActionButton, PepDialogData, PepDialogService } from '@pepperi-addons/ngx-lib/dialog';
import { PepSelectionData } from '@pepperi-addons/ngx-lib/list';
import { MatDialogRef } from '@angular/material/dialog';
import { from } from 'rxjs';
import { AddonService } from 'src/services/addon.service';
import { UtilitiesService } from 'src/services/utilities.service';
import { PepButton } from '@pepperi-addons/ngx-lib/button';


@Component({
  selector: 'query-form',
  templateUrl: './query-form.component.html',
  styleUrls: ['./query-form.component.scss']
})
export class QueryFormComponent implements OnInit {
  
  query: any;
  mode: string;
  temp: string;
  queryUUID: string;
  resourceOptions: Array<PepButton> = [];
  queryLoaded: boolean = false;
  seriesDataSource: IPepGenericListDataSource = this.getSeriesDataSource();

  constructor(
    public addonService: AddonService,
    public translate: TranslateService,
    public genericListService: PepGenericListService,
    public activateRoute: ActivatedRoute,
    private router: Router,
    public dialogService: PepDialogService,
    private utilitiesService: UtilitiesService) { }

   async ngOnInit() {
        this.queryUUID = this.activateRoute.snapshot.params.query_uuid;
        this.resourceOptions = [{key:"all_activities",value:"all_activities"},{key:"transaction_lines",value:"transaction_lines"}]; // need to take from relation
        this.mode = this.router['form_mode']
        this.query = this.emptyQuery();
        this.query.Key = this.queryUUID;
        debugger
        if (this.mode == 'Edit') {
            this.query = (await this.addonService.getDataQueryByKey(this.queryUUID))[0]
        }
        this.queryLoaded = true;
        this.seriesDataSource = this.getSeriesDataSource();


   }

  async saveClicked() {
    debugger
    try {
        debugger
        await this.addonService.upsertDataQuery(this.query);
        const dataMsg = new PepDialogData({
            title: this.translate.instant('Query_UpdateSuccess_Title'),
            actionsType: 'close',
            content: this.translate.instant('Query_UpdateSuccess_Content')
        });
        this.dialogService.openDefaultDialog(dataMsg).afterClosed().subscribe(() => {
            this.goBack();
        });
    }
    catch (error) {
        const errors = this.utilitiesService.getErrors(error.message);
        const dataMsg = new PepDialogData({
            title: this.translate.instant('Query_UpdateFailed_Title'),
            actionsType: 'close',
            content: this.translate.instant('Query_UpdateFailed_Content', {error: errors.map(error=> `<li>${error}</li>`)})
        });
        this.dialogService.openDefaultDialog(dataMsg);
    }
  } 

  goBack() {
    this.router.navigate(['..'], {
        relativeTo: this.activateRoute,
        queryParamsHandling: 'preserve'
    })
  }

  resourceChanged(e){

  }

  showSeriesEditorDialog(){}

  seriesActions: IPepGenericListActions = {
    get: async (data: PepSelectionData) => {
        const actions = [];
        if (data && data.rows.length == 1) {
            actions.push({
                title: this.translate.instant('Edit'),
                handler: async (objs) => {
                    //this.openFieldForm(objs.rows[0]);
                }
            });
            actions.push({
                title: this.translate.instant('Delete'),
                handler: async (objs) => {
                    //this.showDeleteDialog(objs.rows[0]);
                }
            })
        }
        return actions;
    }
  }

  

  getSeriesDataSource() {
    return {
        init: async(params:any) => {
          let series = (await this.addonService.getDataQueryByKey(this.queryUUID)).Series;
            return Promise.resolve({
                dataView: {
                    Context: {
                        Name: '',
                        Profile: { InternalID: 0 },
                        ScreenSize: 'Landscape'
                    },
                    Type: 'Grid',
                    Title: '',
                    Fields: [
                        {
                            FieldID: 'Name',
                            Type: 'TextBox',
                            Title: this.translate.instant('Name'),
                            Mandatory: false,
                            ReadOnly: true
                        },
                        {
                            FieldID: 'Aggregator',
                            Type: 'TextBox',
                            Title: this.translate.instant('Aggregator'),
                            Mandatory: false,
                            ReadOnly: true
                        },
                        {
                            FieldID: 'Field',
                            Type: 'TextBox',
                            Title: this.translate.instant('Field'),
                            Mandatory: false,
                            ReadOnly: true
                        },
                    ],
                    Columns: [
                        {
                            Width: 33
                        },
                        {
                            Width: 33
                        },
                        {
                            Width: 33
                        }
                    ],
                    FrozenColumnsCount: 0,
                    MinimumColumnWidth: 0
                },
                totalCount: series?.length,
                items: series
            });
        },
        inputs: () => {
            return Promise.resolve({
                pager: {
                    type: 'scroll'
                },
                selectionType: 'single',
                noDataFoundMsg: this.translate.instant('Series_NoDataFound')
            });
        },
    } as IPepGenericListDataSource
}


emptyQuery(){
    return {
        Name: '',
        Series: []
    }
}

}
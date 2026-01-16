import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { TranslateService } from '@ngx-translate/core';
import { IPepGenericListActions, IPepGenericListDataSource, IPepGenericListInitData, IPepGenericListPager, PepGenericListService } from "@pepperi-addons/ngx-composite-lib/generic-list";
import { PepSelectionData } from '@pepperi-addons/ngx-lib/list';
import { PepMenuItem } from '@pepperi-addons/ngx-lib/menu';
import { AddonService } from 'src/services/addon.service';

export type FormMode = 'Add' | 'Edit';
export const EMPTY_OBJECT_NAME = 'NewCollection';

@Component({
  selector: 'query-manager',
  templateUrl: './query-manager.component.html',
  styleUrls: ['./query-manager.component.scss']
})
export class QueryManagerComponent implements OnInit {

  dataSource: IPepGenericListDataSource = this.getDataSource();

  pager: IPepGenericListPager = {
      type: 'scroll',
  };
  
  menuItems:PepMenuItem[] = []

  recycleBin: boolean = false;

  constructor(
    public addonService: AddonService,
    public translate: TranslateService,
    public genericListService: PepGenericListService,
    public activateRoute: ActivatedRoute,
    private router: Router
    ) { debugger }

  ngOnInit(): void {
    debugger
    this.menuItems = this.getMenuItems();
  }

  getMenuItems() {
        return [{
            key:'Import',
            text: this.translate.instant('Import'),
            hidden: this.recycleBin
        },
        {
            key: 'RecycleBin',
            text: this.translate.instant('Recycle Bin'),
            hidden: this.recycleBin
        },
        {
            key: 'BackToList',
            text: this.translate.instant('Back to list'),
            hidden: !this.recycleBin
        }]
    }

  getDataSource() {
    const noDataMessageKey = this.recycleBin ? 'RecycleBin_NoDataFound' : 'Query_Manager_NoDataFound'
    return {
        init: async(params:any) => {
            let queries = await this.addonService.getAllQueries();
            if(params.searchString){

            }
            return Promise.resolve({
                dataView: {
                    Context: {
                        Name: '',
                        Profile: { InternalID: 0 },
                        ScreenSize: 'Landscape'
                    },
                    Type: 'Grid',
                    Title: 'Query Manager',
                    Fields: [
                        {
                            FieldID: 'Name',
                            Type: 'TextBox',
                            Title: this.translate.instant('Name'),
                            Mandatory: false,
                            ReadOnly: true
                        },
                        {
                            FieldID: 'Resource',
                            Type: 'TextBox',
                            Title: this.translate.instant('Resource'),
                            Mandatory: false,
                            ReadOnly: true
                        },
                    ],
                    Columns: [
                        {
                            Width: 50
                        },
                        {
                            Width: 50
                        }
                    ],
      
                    FrozenColumnsCount: 0,
                    MinimumColumnWidth: 0
                },
                totalCount: queries.length,
                items: queries
            });
        },
        inputs: () => {
            return Promise.resolve({
                pager: {
                    type: 'scroll'
                },
                selectionType: 'single',
                noDataFoundMsg: this.translate.instant(noDataMessageKey)
            });
        },
    } as IPepGenericListDataSource
}

actions: IPepGenericListActions = {
    get: async (data: PepSelectionData) => {
        const actions = [];
        if (data && data.rows.length == 1) {
            if(this.recycleBin) {
                actions.push({
                    title: this.translate.instant('Restore'),
                    handler: async (objs) => {
                        // await this.collectionsService.restoreCollection(objs.rows[0]);
                        await this.addonService.upsertDataQuery({
                            Name: objs.rows[0],
                            Hidden: false,
                        });
                        this.dataSource = this.getDataSource();
                    }
                })
            }
            else {
                actions.push({
                    title: this.translate.instant('Edit'),
                    handler: async (objs) => {
                        //this.navigateToCollectionForm('Edit', objs.rows[0]);
                    }
                });
                actions.push({
                    title: this.translate.instant('Delete'),
                    handler: async (objs) => {
                        //this.showDeleteDialog(objs.rows[0]);
                    }
                })
                // actions.push({
                //     title: this.translate.instant('Export'),
                //     handler: async (objs) => {
                //         this.exportCollectionScheme(objs.rows[0]);
                //     }
                // })
                actions.push({
                    title: this.translate.instant('Edit data'),
                    handler: async (objs) => {
                        //this.navigateToDocumentsView(objs.rows[0]);
                    }
                })
            }
        }
        return actions;
    }
}

menuItemClick(event: any) {
    switch (event.source.key) {
        case 'RecycleBin':
        case 'BackToList': {
            this.recycleBin = !this.recycleBin;
            setTimeout(() => {
                this.router.navigate([], {
                    queryParams: {
                        recycle_bin: this.recycleBin
                    },
                    queryParamsHandling: 'merge',
                    relativeTo: this.activateRoute,
                    replaceUrl: true
                })
            }, 0); 
            this.dataSource = this.getDataSource(); 
            this.menuItems = this.getMenuItems();
            break;
        }
    }
}

navigateToQueryForm(mode: FormMode, name: string) {
    this.router['form_mode'] = mode;
    this.router.navigate([name], {
        relativeTo: this.activateRoute,
        queryParamsHandling: 'preserve',
        state: {form_mode: 'Edit'}
    })
}

// onSearchChanged($event) {
//     this.searchString = $event.value
//     this.reload();
//   }

}
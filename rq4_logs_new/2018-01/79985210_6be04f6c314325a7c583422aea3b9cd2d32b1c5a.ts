import {Component, EventEmitter, Input, OnInit, Output} from '@angular/core';
import {AppHistory, ApplicationHistory, ApprovalHistoryFilter} from '../../commons/models/reporing-data-models';
import {Router} from '@angular/router';

@Component({
    selector: 'app-history-table',
    templateUrl: './application-history-table.component.html',
    styleUrls: ['./application-history-table.component.scss']
})
export class ApplicationHistoryTableComponent implements OnInit {

    @Input()
    private tableHeader: string;

    @Input()
    private dataSource: AppHistory[];

    @Input()
    private filter: ApprovalHistoryFilter;

    @Output()
    private applicationDetail: ApplicationHistory;

    @Output()
    private onFilterChange: EventEmitter<ApprovalHistoryFilter> = new EventEmitter();

    private operatorApprovals: ApplicationHistory[];
    private subscriptions: ApplicationHistory[];

    private isFilterVisible: boolean;
    private filterString: string;
    private showApprovedOn: string;

    constructor(private router: Router) {
    }

    ngOnInit() {
        this.dataSource = [];
        this.applicationDetail = null;
        this.operatorApprovals = [];
        this.subscriptions = [];
        this.isFilterVisible = true;
        this.filterString = '';
        this.showApprovedOn = 'workFlowHistory:showApprovedOn';
    }

    onNavApplication(id: number) {
        this.router.navigateByUrl('/history/application/' + id);
    }

    onFilterItemAdded() {
        this.filter.filterString = this.filterString;
        this.onFilterChange.emit(this.filter);
    }

    onClear() {
        this.filterString = '';
        this.filter.filterString = this.filterString;
        this.onFilterChange.emit(this.filter);
    }
}
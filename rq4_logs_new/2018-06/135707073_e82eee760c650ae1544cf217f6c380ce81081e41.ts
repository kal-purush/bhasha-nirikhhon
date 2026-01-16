
import { Component, OnInit, ViewChild, AfterViewChecked, OnChanges, SimpleChanges, AfterViewInit } from '@angular/core';
import { MatPaginator, MatTableDataSource, MatSort, MatCell } from '@angular/material';
import { Router } from '@angular/router';
import { AppService } from '../app.service';


@Component({
    selector: 'app-dashboard',
    templateUrl: './dashboard.component.html',
    styleUrls: ['./dashboard.component.css']
})

export class DashboardComponent implements AfterViewInit {

    displayedColumns = ['id', 'name', 'progress', 'color'];
    dataSource: MatTableDataSource<UserData>;

    @ViewChild(MatPaginator) paginator: MatPaginator;
    @ViewChild(MatSort) sort: MatSort;
    users: UserData[];
    constructor(private router: Router, private userService: AppService) {
        // Create 100 users
        this.users = [];
        for (let i = 1; i <= 100; i++) { this.users.push(createNewUser(i)); }

        // Assign the data to the data source for the table to render
        this.dataSource = new MatTableDataSource(this.users);
        //  alert('constructor');
    }

    ngAfterViewInit() {
        this.dataSource.paginator = this.paginator;
        this.dataSource.sort = this.sort;
    }


    // ngOnInit(): void {
    //     this.userService.populateDashboard().subscribe(
    //       data => {
    //       //  this.dataSource.data = data;
    //       this.dataSource = new MatTableDataSource(this.users);
    //       },
    //        (err) => {
    //         if (err) { alert('Seesion Expired, login again!'); this.router.navigateByUrl('/login');
    //     }
    //   }
    //     );
    // }

    applyFilter(filterValue: string) {
        filterValue = filterValue.trim(); // Remove whitespace
        filterValue = filterValue.toLowerCase(); // Datasource defaults to lowercase matches
        this.dataSource.filter = filterValue;
        //   alert('apply filter');
    }
}

/** Builds and returns a new User. */
function createNewUser(id: number): UserData {
    const name = 'Dr. ' +
        NAMES[Math.round(Math.random() * (NAMES.length - 1))] + ' ' +
        NAMES[Math.round(Math.random() * (NAMES.length - 1))];
    // alert('return new user');
    return {
        id: id.toString(),
        name: name,
        progress: Math.round(Math.random() * 100).toString(),
        color: COLORS[Math.round(Math.random() * (COLORS.length - 1))]
    };
}

/** Constants used to fill up our data base. */
const COLORS = ['maroon', 'red', 'orange', 'yellow', 'olive', 'green', 'purple',
    'fuchsia', 'lime', 'teal', 'aqua', 'blue', 'navy', 'black', 'gray'];
const NAMES = ['Monica', 'Abhi', 'Abhishek', 'Ashish', 'Amit', 'Jack',
    'Mukesh', 'Thanos', 'Isha', 'Oliver', 'Isabella', 'Jasper',
    'Mikesh', 'Leela', 'Vinay', 'Atul', 'Mia', 'Thomas', 'Elizabeth'];

export interface UserData {
    id: string;
    name: string;
    progress: string;
    color: string;
}
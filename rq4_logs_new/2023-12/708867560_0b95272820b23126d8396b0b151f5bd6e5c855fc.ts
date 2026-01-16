import {AfterViewInit, Component, ViewChild} from '@angular/core';
import {MatTableDataSource, MatTableModule} from '@angular/material/table';
import {RestApiService} from "../../shared/services/rest-api.service";
import {MatPaginator, MatPaginatorModule} from "@angular/material/paginator";
import {LowerCasePipe, NgForOf, NgIf} from "@angular/common";
import {RouterLink, RouterLinkActive} from "@angular/router";
import { AuthService } from '../../shared/services/auth.service';
import {navBarDate} from "../dashboard/navbarData";

export interface userData {
  login: string;
  position: number;
  id: number;
  url: string;
  img: any;
}

const ELEMENT_DATA: userData[] = []



@Component({
  selector: 'app-rest-api',
  templateUrl: './rest-api.component.html',
  styleUrls: ['./rest-api.component.css'],
  standalone: true,
  providers: [RestApiService],
  imports: [MatTableModule, MatPaginatorModule, LowerCasePipe, NgForOf, NgIf, RouterLinkActive, RouterLink],
})
export class RestApiComponent implements AfterViewInit {
  public readonly navBarDate = navBarDate;
  displayedColumns: string[] = ['position', 'login', 'img', 'id', 'url'];
  dataSource = new MatTableDataSource<userData>(ELEMENT_DATA);
  @ViewChild(MatPaginator) paginator: MatPaginator;
  ngAfterViewInit() {
    this.dataSource.paginator = this.paginator;
  }
  constructor
  (private restApiService: RestApiService,
   private authService: AuthService
  ) {
  this.restApiService.showUserPage()
    .subscribe(
      (userData) => {
        this.dataSource.data = userData
        console.log(userData)
      }
    )
  }
  signOut() {
    this.authService.signOut();
  }
}
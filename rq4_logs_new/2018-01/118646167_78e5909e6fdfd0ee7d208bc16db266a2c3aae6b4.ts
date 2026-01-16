import {Component, OnInit} from '@angular/core';
import {Group, User} from "../_models/index";
import {AlertService, GroupService, UserService} from "../_services/index";
import {ActivatedRoute, Router} from "@angular/router";
import 'rxjs/add/operator/filter';


@Component({
    moduleId: module.id,
    templateUrl: 'groupdetails.component.html'
})

export class GroupdetailsComponent implements OnInit {
    currentUser: User;
    group: Group;
    users: User[];
    groupName: string;
    loading = false;

    constructor(private groupService: GroupService,
                private userService: UserService,
                private route: ActivatedRoute,
                private alertService: AlertService,
                private router: Router) {
        this.currentUser = JSON.parse(localStorage.getItem('currentUser'));
    }

    ngOnInit() {
        this.getGroup();
        this.loadAllUsers();
        this.route.queryParams
            .filter(params => params.groupname)
            .subscribe(params => {
                this.groupName = params.groupname;
            });
    }

    private getGroup() {
        this.groupService.getAll().subscribe( groups => {
            for (let group of groups) {
                if (group.groupname === this.groupName) {
                    this.group = group;
                }
            }
        });
    }

    private loadAllUsers() {
        this.userService.getAll().subscribe(users => { this.users = users; });
    }

    private addUserAtGroup(username: string) {
        this.group.users.push( this.users.find( function(user) {
            return user.username === username;
        }));
        this.groupService.update(this.group)
            .subscribe(
            data => {
                this.alertService.success( username + ' ajouté au groupe', true);
                this.router.navigate(['/group', { groupname: this.groupName } ]);
            },
            error => {
                this.alertService.error(error);
                this.loading = false;
            });
    }

    private deleteUserAtGroup(username: string) {
        this.group.users = this.group.users.filter(user => user.username !== username)
        this.groupService.update(this.group)
            .subscribe(
                data => {
                    this.alertService.success( username + ' supprimé au groupe', true);
                    this.router.navigate(['/group', { groupname: this.groupName } ]);
                },
                error => {
                    this.alertService.error(error);
                    this.loading = false;
                });
    }

    private userInGroup(username: string){
        if (username === this.group.adminname) {
            return true;
        }
        for (let user of this.group.users) {
            if (user.username === username){
                return true;
            }
        }
        return false;
    }
}
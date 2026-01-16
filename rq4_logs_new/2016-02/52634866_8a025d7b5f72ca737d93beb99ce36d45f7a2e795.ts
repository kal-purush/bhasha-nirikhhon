import {Component, ChangeDetectionStrategy} from "angular2/core";
import {StringFilter} from "./stringfilter";

@Component({
    selector: "Home",
    template: `
    <div class="container-fluid">
  <div class="row">
    <div class="col-sm-3 col-sm-offset-2">
      <h2>A list of teams: </h2>
      <ul>
        <li *ngFor="#teamName of teams | stringfilter:fTeam">{{teamName}}</li>
      </ul>
    </div>
  </div>
  <div class="row">
    <form class="form-horizontal col-sm-12">
      <div class="form-group">
        <label for="fTeam" class="control-label col-sm-2">Filter teams: </label>
        <div class="col-sm-2">
          <input [(ngModel)]="fTeam" class="form-control" id="fTeam" type="text"/>
        </div>
      </div>
      <div class="form-group">
        <label for="newTeam" class="control-label col-sm-2">Add team: </label>
        <div class="col-sm-2">
          <input [(ngModel)]="newTeam" class="form-control" id="newTeam" type="text"/>
        </div>
        <div class="col-sm-2">
         <button (click)="addTeam(newTeam)" id="addBtn" class="btn btn-small">
          Add team
         </button>
        </div>
      </div>
      <div class="form-group">
        <label for="deleteTeam" class="control-label col-sm-2">
          Remove a team:
        </label>
        <div class="col-sm-2">
          <select class="form-control"
            [(ngModel)]="team"
            ngControl="deleteTeam"
            #deleteTeam="ngForm" >
            <option *ngFor="#t of teams" [value]="t">{{t}}</option>
          </select>
        </div>
         <button (click)="removeTeam(team)" id="delBtn" class="btn btn-small">
          Delete team
         </button>
      </div>
      <div class="form-group">
        <div class="col-sm-1 col-sm-offset-2">
          <!-- Challenge: restore the dropped teams, set the button here
           Add the usual classes to the button
           -->
        </div>
      </div>
    </form>
  </div>
</div>
  `,
    styles: [`
    .selected {
      background-color: #CFD8DC !important;
      color: white;
    }
  `],
    directives: [],
    pipes: [StringFilter],
    changeDetection: ChangeDetectionStrategy.CheckAlways
})

export class HomeComponent {
    public teams:string[] = ['Mets', 'Yankees', 'Red Sox', 'Phillies', 'Blue Jays',
        'Braves', 'Orioles', 'Marlins', 'Rays', 'Nationals'];
    public fTeam:string = "";

    addTeam(team:string) {
        this.teams.push(team);
    }

    removeTeam(team:string) {
        var index = this.teams.indexOf(team);
        this.teams.splice(index, 1);
    }

    //onSelect(link:Link) {
    //    this.selectedLink = link;
    //}
}
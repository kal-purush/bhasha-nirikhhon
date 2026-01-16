import { Recipe } from '../models/recipe';
import { Component, Input } from '@angular/core';

@Component({
    selector: 'random-recipe',
    template: `<div class="modal-dialog">
        <div *ngIf="!rolling">
            <!-- Modal content-->
            <div class="modal-content  random-recipe" *ngIf="randomRecipe">
                <div class="modal-header" style="background-color:#022f32;">
                    <button type="button" class="close" style="color:white;" data-dismiss="modal">&times;</button>
                    <h4 class="modal-title" style="color:white;">{{randomRecipe.name}}</h4>
                </div>
                <div class="modal-body">
                    <div>
                        <!--<rating [(ngModel)]="randomRecipe.ratingaverage" [readonly]="true"></rating>-->
                        ({{randomRecipe.ratingcount}} ratings)
                        <div style="float:right;margin-top:10px;" [ngSwitch]="randomRecipe.level.id"><span *ngSwitchCase="3" class="tag tag-danger">Difficult</span>
                            <span *ngSwitchCase="1" class="tag tag-success">Easy</span> <span *ngSwitchCase="2" class="tag tag-warning">Medium</span>
                        </div>
                    </div>
                    <p><b>{{randomRecipe.duration.range}}</b></p>
                    <p><i>{{randomRecipe.introduction}}</i></p>
                    <p><a routerLink="/recipes/{{randomRecipe.id}}" data-dismiss="modal">Go to recipe
            <span class="glyphicon glyphicon-menu-right"></span><span class="glyphicon glyphicon-menu-right"></span></a>
                    </p> <span *ngIf="randomRecipe.coverpicture"><img class="random-image img-responsive"
                                                            src="{{baseUrl}}{{randomRecipe.coverpicture.picture.medium.url}}"></span>
                    <span *ngIf="!randomRecipe.coverpicture"><img class="random-image img-responsive"
                                                        src="assets/cookie.jpg"></span></div>
                <div class="modal-footer">
                    <p style="float:left;">Random Recipe</p><br><br> <span style="float:left;" *ngIf="randomRecipe.coverpicture"><p>Picture &copy;
          {{randomRecipe.coverpicture.author}}</p></span>
                    <!-- <button type="button" class="btn btn-default" (click)="reRoll()">Reroll</button> -->
                    <button type="button" class="btn btn-default" data-dismiss="modal">Close
          </button>
                </div>
            </div>
        </div>
        <div *ngIf="rolling">
            <div class="modal-content roller"><i class="fa fa-refresh fa-spin"></i></div>
        </div>
    </div>`,
    styleUrls: ['./recipe-list.component.scss']
})
export class RandomRecipeModalComponent {
    @Input() randomRecipe: Recipe;
}
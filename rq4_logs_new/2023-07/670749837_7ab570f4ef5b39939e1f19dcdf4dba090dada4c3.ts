import { Component, OnInit } from '@angular/core';
import Category from 'src/models/Category';
import Layer from 'src/models/Layer';
import Recipe from 'src/models/Recipe';
import RecipeService from 'src/services/recipe.service';
import categoryService from 'src/services/category.service';
import User from 'src/models/User';
import { Router } from '@angular/router';

@Component({
  selector: 'app-add-recipe',
  templateUrl: './add-recipe.component.html',
  styleUrls: ['./add-recipe.component.scss']
})
export class AddRecipeComponent implements OnInit {
  arrCategory: Category[];
  constructor(public catSer: categoryService, public recipeSer: RecipeService, public route: Router) {

  }
  myRecipe: Recipe = new Recipe(0, "", 0, 0, 0, new Date(1, 1, 2000), [new Layer("", [])], [], 0, "../../assets/images/ספייסי-שוקו.jpg", false);
  numLayer: number;
  user: User;
  addLayer(name) {
    this.myRecipe.Layers[this.numLayer++] = new Layer(name, [])
  }
  addInstruction(instruction) {
    this.myRecipe.Preparation.push(instruction.value);
    instruction.value = "";
  }
  addComponent(component) {
    console.log("addComponent")
    this.myRecipe.Layers[this.numLayer - 1].Components.push(component.value)
    component.value = "";
    console.log(this.myRecipe.Layers)
  }
  save(form) {
    if (this.myRecipe.Image == "")
      this.myRecipe.Image = "18361.jpg"
    this.myRecipe.AddDate = new Date(Date.now())
    console.log(this.myRecipe.AddDate)
    this.myRecipe.UserId = JSON.parse(localStorage.getItem("user")).Id;
    this.myRecipe.IsDisplay = true;
    this.recipeSer.addRecipe(this.myRecipe).subscribe(succ => { this.myRecipe = succ; console.log(this.myRecipe.AddDate) });


    form.reset();
    alert("תודה ששיתפת אותנו ממטבחך😋")
    this.route.navigate([""])
  }
  ngOnInit(): void {
    this.catSer.GetAllCategories().subscribe(succ => { this.arrCategory = succ });
    this.numLayer = 0;
    this.user = JSON.parse(localStorage.getItem("user"))
  }
}
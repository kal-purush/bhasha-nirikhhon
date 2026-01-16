import IMetier from "@/interfaces/IMetier";
import { Component, Input, OnInit } from "@angular/core";
import { environment } from "../../../../../environments/environment";

@Component({
  selector: 'app-evaluation-metier-item',
  templateUrl: './evaluation-metier-item.component.html',
  styleUrls: ['./evaluation-metier-item.component.scss']
})
export class EvaluationMetierItemComponent implements OnInit {

  @Input() metier$ : IMetier = null;

  onSelectCard() {
    this.metier$.isChecked = !this.metier$.isChecked;
  }
  constructor() { }

  ngOnInit(): void {
    this.metier$.isChecked =false;
  }
  calculateColor = (id : number) => {
    let colors: string[] = environment.evaluerIHM.gradientColors;
    return colors[id%colors.length];
  }

}
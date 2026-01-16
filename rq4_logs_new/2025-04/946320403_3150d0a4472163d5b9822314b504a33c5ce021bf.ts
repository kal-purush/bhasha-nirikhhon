import { Stage } from './../../../model/model';

import { CommonModule } from '@angular/common';
import { Component } from '@angular/core';
import { FormsModule, NgForm, NgModel } from '@angular/forms';
import { SideBarEtudiantComponent } from '../../side-bar-etudiant/side-bar-etudiant.component';

import { StageServiceService } from '../../../service/stage-service.service';
import { TypeStage } from '../../../model/enums';
import { Router } from '@angular/router';

@Component({
  
  selector: 'app-add-stage',
  standalone: true,
  imports: [FormsModule, CommonModule, SideBarEtudiantComponent],
  templateUrl: './add-stage.component.html',
  styleUrl: './add-stage.component.css'
})
export class AddStageComponent {
   // Modèle pour stocker les données du formulaire
   stage: Stage = {
     nomEntreprise: '',
     adresseEntreprise: '',
     numEncadrant: '',
     nomEncadrant: '',
     intituleSujet: '',
     descriptionSujet: '',
     typeStage: TypeStage.PFA // Valeur par défaut
    
   };
  constructor(private stageService: StageServiceService,private router: Router) {}
  
  onSubmit(form: NgForm) {
    if (form.valid) {
      this.stageService.addStage(this.stage).subscribe({
        
        next: (response: any) => {
          console.log('Stage ajouté avec succès !', response);
          form.reset(); // Réinitialiser le formulaire après soumission
          this.router.navigate(['/listeStages']);
        },
        error: (err: any) => {
          console.error('Erreur lors de l\'ajout du stage :', err);
        }
      });
    }
  }

}
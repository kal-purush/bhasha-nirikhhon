import {Pipe, PipeTransform} from '@angular/core';

// Tell Angular2 we're creating a Pipe with TypeScript decorators
@Pipe({
  name: 'CollaboratorsPipe'
})
export class CollaboratorsPipe implements PipeTransform {

  // Transform is the new "return function(value, args)" in Angular 1.x
  transform(value, searchText : String) {

    if(value && searchText) {
      return value.filter(collaborator => {
        //Si le texte correspond a un nom ou a un prénom, on retourne true
        if(collaborator.name.toUpperCase().indexOf(searchText.toUpperCase())!= -1
          || collaborator.surname.toUpperCase().indexOf(searchText.toUpperCase())!= -1) {
            return true;
          }

        //Sinon on va regarder parmi les compétences du collaborateur courant
        if(collaborator.competences) {
          for(let comp of collaborator.competences) {
            if(comp) {
              //Si il y a une correspondance, on s'arrête sinon on continue à chercher
              if (comp.type.toUpperCase().indexOf(searchText.toUpperCase())!= -1
                || comp.name.toUpperCase().indexOf(searchText.toUpperCase())!= -1) {
                  return true;
                }
            }
          }
        }
        return false;
      });
    } else {
      return value;
    }
  }
}
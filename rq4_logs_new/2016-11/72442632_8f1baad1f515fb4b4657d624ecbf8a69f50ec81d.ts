import { Component, Input } from '@angular/core'

import { ServiceAttrsData } from './../../../../services/attrs_data/attrs_data.service'

import { Category } from './../../../../objects/categorie'
import { AttrData } from './../../../../objects/attr_data'

@Component ({
	selector: 'my-selected-attrs-data',
	templateUrl: 'selected_attrs_data.component.html'
})

export class SelectedAttrsData {
	@Input() category: Category
	attrs_data: AttrData[] = []
	newAttrData: string

	constructor(
		private serviceAttrData: ServiceAttrsData
	) {}

	addAttrData(newAttrData, cat) {
		
		let catId = cat._id

		newAttrData = newAttrData.trim(); //On enlève les espaces blanc s'il y en a

		//On regarde si newCategory a bien une valeur non nulle
		if(!newAttrData) {
			return ;
		}

		//On effectue une requête http pour ajouter une nouvelle catégorie via le service categories
		this.serviceAttrData.createAttrData(newAttrData, catId)
		.then(newAttrData => {
			this.attrs_data.push(newAttrData) //On ajoute la catégorie à la liste actuelle
			this.newAttrData = '' //On remet le champ des catégories à zéro une fois l'opération effectuée
		},
		function() {
			console.log('Il y a un problème');
		})
	}
}
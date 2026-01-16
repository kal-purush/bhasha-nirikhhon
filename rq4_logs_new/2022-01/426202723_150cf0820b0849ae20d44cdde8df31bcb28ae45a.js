const { createCanvas } = require('canvas');
const { writeFileSync } = require('fs');

class GraphElement {
	constructor(label, flatValue, prctValue) {
		this.label = label;
		this.flatValue = flatValue;
		this.prctValue = prctValue;
	}
}
class Graph {
	constructor(liste,correctIndex,unit) {
		const max20char = (texte) => (texte.length <= 20) ? texte : (texte.substring(0,16) + ' ...');

		// Création
		this._canvas = createCanvas(650, liste.length * 50 );
		this._dessin = this._canvas.getContext('2d');

		// Initialisation
		this._dessin.lineJoin = 'round';
		this._dessin.lineWidth = 20;

		// Réponse majoritaire
		let bestIndex = liste.reduce( (bestI,nowV,nowI,arr) => (arr[bestI].flatValue<nowV.flatValue)?nowI:bestI , 0 );

		// Dessin
		for(let i=0 ; i<liste.length ; i++) {
			// Jauge - background
			this._dessin.strokeStyle = '#242424';
			this._dessin.strokeRect( 220 , i*50 + 10 , 250 , 10 );

			// Jauge - forground
			if( liste[i].flatValue != 0 ) {
				if( i === correctIndex ) {
					this._dessin.strokeStyle = 'lime';
				} else
				if( i === bestIndex ) {
					this._dessin.strokeStyle = 'gold';
				} else
				{
					this._dessin.strokeStyle = 'teal';
				}
				this._dessin.strokeRect( 220 , i*50 + 10 , 250 * liste[i].prctValue, 10 );
			}

			// Texte résultat
			this._dessin.fillStyle = 'white';
			this._dessin.font = '24px "arial"';
			this._dessin.fillText(`${ liste[ i ].flatValue } ${unit} (${ Math.round( liste[ i ].prctValue *10000 )/100}%)` , 500 , (i+1)*50 - 25);

			// Texte réponse
			this._dessin.fillStyle = 'white';
			this._dessin.font = '20px "arial"';
			this._dessin.fillText( max20char( liste[i].label ) , 10 , (i+1)*50 - 25);
		}
	}

	write(name) {
		let path = `./data/renduSondage_${name}.png`;
		writeFileSync( path ,this._canvas.toBuffer());
		return path;
	}
}

module.exports = { Graph , GraphElement };
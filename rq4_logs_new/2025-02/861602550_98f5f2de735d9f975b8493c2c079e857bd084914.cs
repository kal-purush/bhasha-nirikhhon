
//classe bouteille

namespace Bouteille {
	public class Bouteille {

		private float contenanceEnLitre { get; set; };
		private float contenuEnLitre { get; set; };
		private bool estOuverte { get; set; };
		private string typeLiquide { get; set; };




		public Bouteille(){
			contenanceEnLitre = 1.0f;
			contenuEnLitre = 1.0f;
			estOuverte= false;
			typeLiquide = "eau";


		}

		public Bouteille(float _contenanceEnLitre, float _contenuEnLitre, bool _ouverte, string _typeLiquide){
            contenanceEnLitre = _contenanceEnLitre;
            contenuEnLitre = _contenuEnLitre;
            estOuverte = _ouverte;
            typeLiquide = _typeLiquide;
        }

		public Bouteille(Bouteille bouteilleACopier){
            contenanceEnLitre = bouteilleACopier.contenanceEnLitre;
            contenuEnLitre = bouteilleACopier.contenuEnLitre;
            estOuverte = bouteilleACopier.estOuverte;
            typeLiquide = bouteilleACopier.typeLiquide;
        }

		public bool Fermer(){
			if (estOuverte) {
				estOuverte = false;
			}
			
		}

		public bool Ouvrir()
		{
			if (!estOuverte) {
				estOuverte = true; 
			}
		}
		/*
		public bool Remplir(){

			
		}

		/// 
		/// <param name="quantiteEnLitre"></param>
		public bool Remplir(float quantiteEnLitre){

			
		}

		public bool Vider(){

			return false;
		}

		/// 
		/// <param name="quantiteEnLitre"></param>
		public bool Vider(float quantiteEnLitre){

			return false;
		}
		*/

	}//end Bouteille

}//end namespace Solution1
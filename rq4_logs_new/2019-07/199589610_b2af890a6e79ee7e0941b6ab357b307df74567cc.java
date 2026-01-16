import java.util.*;
import java.util.stream.Collectors;

public class UtilitaireRecherche {
        HashMap<String, String[]> Corpus = new HashMap<String, String[]>();

        public UtilitaireRecherche() {
        }

        // Enregistrer les documents dans le corpus
        public void AjouterAuCorpus(String nomDocument, String[] document) {
                Corpus.put(nomDocument, document);
        }

        // Calcul et obtention des valeurs Tf des termes du corpus
        public HashMap<String, Integer> ObtenirTf() {
                HashMap<String, Integer> resultat = new HashMap<String, Integer>();

                for (String nomDocument : Corpus.keySet()) {
                        String[] termesDocument = Corpus.get(nomDocument);

                        if (termesDocument.length > 0) {

                                List<String> termesDistinct = Arrays.stream(termesDocument).distinct()
                                                .collect(Collectors.<String>toList());
                                for (Object terme : termesDistinct) {
                                        String termeStr = (String) terme;
                                        if (!resultat.containsKey(terme))
                                                resultat.put(termeStr, 1);
                                        else
                                                resultat.put(termeStr, resultat.get(termeStr) + 1);
                                }
                        }
                }

                return resultat;
        }

        // Calcul et obtention du vecteur Idf des termes du corpus
        public HashMap<String, Double> ObtenirIdf() {

                HashMap<String, Double> resultat = new HashMap<String, Double>();
                HashMap<String, Integer> termesEtTf = ObtenirTf();
                Integer nombreDocument = Corpus.size();

                for (String terme : termesEtTf.keySet()) {
                        Integer tf = termesEtTf.get(terme);
                        Double idf = Math.log(nombreDocument * 1.0 / tf * 1.0) / Math.log(2);
                        resultat.put(terme, idf);
                }

                return resultat;
        }

        // Calcul et obtention du vecteur TfIdf pour tous les documents du corpus
        public HashMap<String, Vector<Double>> ObtenirTousVecteursTfIdf() {
                HashMap<String, Vector<Double>> resultat = new HashMap<String, Vector<Double>>();

                for (String nomDocument : Corpus.keySet()) {
                        String[] termesDocument = Corpus.get(nomDocument);

                        HashMap<String, Vector<Double>> vecteursTfIdf = ObtenirVecteurTfIdf(nomDocument,
                                        termesDocument);

                        resultat.put(nomDocument, vecteursTfIdf.get(nomDocument));
                }

                return resultat;
        }

        // Calcul et obtention du vecteur TfIdf pour un document
        public HashMap<String, Vector<Double>> ObtenirVecteurTfIdf(String nomDocument, String[] document) {
                HashMap<String, Vector<Double>> resultat = new HashMap<String, Vector<Double>>();

                Vector<Double> ValeursTfIdf = new Vector<Double>();

                if (document.length > 0) {
                        HashMap<String, Double> termesIdf = ObtenirIdf();

                        for (String termeUnique : termesIdf.keySet()) {
                                ValeursTfIdf.add(0D);
                                for (String termeDoc : document) {
                                        if (termeUnique == termeDoc) {
                                                Long tf = Arrays.stream(document).filter(d -> d == termeDoc).count();
                                                Double facteurIdf = termesIdf.get(termeUnique);

                                                ValeursTfIdf.set(ValeursTfIdf.size() - 1, tf * 1.0 * facteurIdf * 1.0);

                                                break;
                                        }
                                }
                        }
                        resultat.put(nomDocument, ValeursTfIdf);
                }

                return resultat;
        }

        // Calcul et obtention de la similarite etre deux vecteurs
        public static Double ObtenirSimilarite(Vector<Double> vecteur1, Vector<Double> vecteur2) {

                Double similarite = 0D;
                if (vecteur1.size() == vecteur2.size()) {
                        Double produitVecteurs = 0.0;
                        Double norm1 = 0.0;
                        Double norm2 = 0.0;
                        for (int i = 0; i < vecteur1.size(); i++) {
                                produitVecteurs += vecteur1.get(i) * vecteur2.get(i);
                                norm1 += Math.pow(vecteur1.get(i), 2);
                                norm2 += Math.pow(vecteur2.get(i), 2);
                        }
                        similarite = produitVecteurs / (Math.sqrt(norm1) * Math.sqrt(norm2));
                }

                return similarite;
        }

        // Calcul su score de Hiemstra et al. simplifié
        public HashMap<String, Double> ScoresHiemstra(String[] termesRequete) {
                HashMap<String, Double> resultat = new HashMap<String, Double>();

                for (String nomDocument : Corpus.keySet()) {
                        String[] termesDocument = Corpus.get(nomDocument);

                        Double scoreHiemstra = ScoreHiemstra(nomDocument, termesDocument, termesRequete);

                        resultat.put(nomDocument, scoreHiemstra);
                }

                return resultat;
        }

        // Calcul et obtention du score Hiemstra pour un document
        public Double ScoreHiemstra(String nomDocument, String[] document, String[] termesRequete) {

                Double score = 1D;

                if (document.length > 0) {

                        Integer nombreTermesCorpus = 0;
                        for (String nomDoc : Corpus.keySet()) {
                                nombreTermesCorpus += Corpus.get(nomDoc).length; // |C|
                        }

                        Integer nombreTermesDocument = document.length; // |D|

                        for (String terme : termesRequete) {
                                Long tf = Arrays.stream(document).filter(d -> d == terme).count(); // nombre de fois que
                                                                                                   // le terme t
                                Double vraisemblanceMaximale = tf * 1.0 / nombreTermesDocument * 1.0;
                                score *= vraisemblanceMaximale;

                        }
                        score *= (nombreTermesDocument * 1.0 / nombreTermesCorpus * 1.0);
                }

                return score;
        }

        public static void main(String[] args) {
                ExecuteSansTruncatureA3Caractere();
                ExecuteAvecTruncatureA3Caractere();
        }

        public static void ExecuteSansTruncatureA3Caractere() {
                System.out.println("====================================================");
                System.out.println("================ Sans truncature à 3 ===============");
                System.out.println("====================================================");

                String[] D1 = { "Lucie", "crayon", "roule" };
                String[] D2 = { "maison", "rouge" };
                String[] D3 = { "crayon", "rouge", "maison", "rouge" };
                String[] D4 = { "policier", "rouge", "congé", "soulier", "rouge" };

                UtilitaireRecherche utilitaireRecherche = new UtilitaireRecherche();

                // Enregistrement des documents
                utilitaireRecherche.AjouterAuCorpus("D1", D1);
                utilitaireRecherche.AjouterAuCorpus("D2", D2);
                utilitaireRecherche.AjouterAuCorpus("D3", D3);
                utilitaireRecherche.AjouterAuCorpus("D4", D4);
                String[] requete = { "maison", "rouge" };

                mainCommun(utilitaireRecherche, requete);
        }

        public static void ExecuteAvecTruncatureA3Caractere() {

                System.out.println("====================================================");
                System.out.println("================ Avec Truncature à 3 ===============");
                System.out.println("====================================================");

                String[] D1_ = { "Luc", "cra", "rou" };
                String[] D2_ = { "mai", "rou" };
                String[] D3_ = { "cra", "rou", "mai", "rou" };
                String[] D4_ = { "pol", "rou", "con", "sou", "rou" };

                UtilitaireRecherche utilitaireRecherche = new UtilitaireRecherche();

                // Enregistrement des documents
                utilitaireRecherche.AjouterAuCorpus("D1", D1_);
                utilitaireRecherche.AjouterAuCorpus("D2", D2_);
                utilitaireRecherche.AjouterAuCorpus("D3", D3_);
                utilitaireRecherche.AjouterAuCorpus("D4", D4_);
                String[] requete = { "mai", "rou" };

                mainCommun(utilitaireRecherche, requete);
        }

        public static void mainCommun(UtilitaireRecherche utilitaireRecherche, String[] requete) {

                // Calcul et affichage des Idf
                HashMap<String, Double> termesIdf = utilitaireRecherche.ObtenirIdf();
                for (String terme : termesIdf.keySet()) {
                        System.out.println("Le terme:" + terme + " a pour facteur idf: "
                                        + termesIdf.get(terme).toString());
                }
                System.out.println("_________________________________________");

                // Calcul des Vecteurs TfIdf
                HashMap<String, Vector<Double>> vecteursTfIdf = utilitaireRecherche.ObtenirTousVecteursTfIdf();
                // Affichage des Vecteurs TfIdf
                Set<String> termesUnique = termesIdf.keySet();
                AfficheVecteurTfIdf(vecteursTfIdf, termesUnique);
                System.out.println("_________________________________________");

                // Calcul et affichage de la similarité entre deux vecteurs
                AfficheSimilariteVecteurs("D1", "D2",
                                ObtenirSimilarite(vecteursTfIdf.get("D1"), vecteursTfIdf.get("D2")));
                AfficheSimilariteVecteurs("D1", "D3",
                                ObtenirSimilarite(vecteursTfIdf.get("D1"), vecteursTfIdf.get("D3")));
                AfficheSimilariteVecteurs("D1", "D4",
                                ObtenirSimilarite(vecteursTfIdf.get("D1"), vecteursTfIdf.get("D4")));
                AfficheSimilariteVecteurs("D2", "D3",
                                ObtenirSimilarite(vecteursTfIdf.get("D2"), vecteursTfIdf.get("D3")));
                AfficheSimilariteVecteurs("D2", "D4",
                                ObtenirSimilarite(vecteursTfIdf.get("D2"), vecteursTfIdf.get("D4")));
                AfficheSimilariteVecteurs("D3", "D4",
                                ObtenirSimilarite(vecteursTfIdf.get("D3"), vecteursTfIdf.get("D4")));
                System.out.println("_________________________________________");

                // Document jugé pertinent pour la requête par mots-clefs « maison rouge » (« mai rou »)
                HashMap<String, Vector<Double>> vecteurTfIdfRequete = utilitaireRecherche
                                .ObtenirVecteurTfIdf("Requete1", requete);
                HashMap<String, Double> documentsEtSimilarites = new HashMap<String, Double>();
                documentsEtSimilarites.put("D1",
                                ObtenirSimilarite(vecteurTfIdfRequete.get("Requete1"), vecteursTfIdf.get("D1")));
                documentsEtSimilarites.put("D2",
                                ObtenirSimilarite(vecteurTfIdfRequete.get("Requete1"), vecteursTfIdf.get("D2")));
                documentsEtSimilarites.put("D3",
                                ObtenirSimilarite(vecteurTfIdfRequete.get("Requete1"), vecteursTfIdf.get("D3")));
                documentsEtSimilarites.put("D4",
                                ObtenirSimilarite(vecteurTfIdfRequete.get("Requete1"), vecteursTfIdf.get("D4")));

                Double similariteMax = 0D;
                String documentPertinent = "";
                for (String document : documentsEtSimilarites.keySet()) {
                        Double similarite = documentsEtSimilarites.get(document);

                        if (similarite > similariteMax) {
                                similariteMax = similarite;
                                documentPertinent = document;
                        }
                }
                System.out.println("Le document jugé partinent est: " + documentPertinent);
                System.out.println("_________________________________________");

                // Calcul des scores de Hiemstra et al. simplifié pour la requête {"maison",
                // "rouge"}
                HashMap<String, Double> scores = utilitaireRecherche.ScoresHiemstra(requete);
                AfficheScoreHiemstra(scores);
        }

        // Formater et Afficher un vecteur TfIdf d'un document
        private static void AfficheVecteurTfIdf(HashMap<String, Vector<Double>> vecteurs, Set<String> termesUnique) {

                for (String nomDocument : vecteurs.keySet()) {
                        Vector<Double> vecteursTfIdf = vecteurs.get(nomDocument);

                        System.out.println("Le vecteur Tf.Idf pour le document " + nomDocument + " est: ");
                        if (vecteursTfIdf.size() > 0) {
                                int count = 0;
                                for (String terme : termesUnique) {
                                        System.out.println(terme + " = " + vecteursTfIdf.get(count++));
                                }
                        }
                }
        }

        // Formater et Afficher la similarité entre deux vecteurs
        private static void AfficheSimilariteVecteurs(String nomVecteur1, String nomVecteur2, Double similarite) {
                System.out.println("La similarité entre les vecteurs: " + nomVecteur1 + " et " + nomVecteur2 + " est: "
                                + similarite);
        }

        // Formater et Afficher un vecteur TfIdf d'un document
        private static void AfficheScoreHiemstra(HashMap<String, Double> scores) {

                System.out.println("Affichage des scores de Hiemstra");

                for (String nomDocument : scores.keySet()) {
                        System.out.println("Les scores de Hiemstra pour le document " + nomDocument + " est: "
                                        + scores.get(nomDocument));
                }
        }
}
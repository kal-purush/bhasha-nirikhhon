################################################################################
# On importe les classes et fonctions nécessaires pour créer un serveur HTTP et
# pour analyser les données issues des requêtes.
################################################################################
from http.server import BaseHTTPRequestHandler, HTTPServer  # BaseHTTPRequestHandler et HTTPServer pour créer un serveur
import urllib.parse  # Permet de décoder les données envoyées via la méthode POST (formulaire)
import time          # Permet de gérer le temps (chronomètre)

################################################################################
# Ci-dessous, on définit une liste nommée "verbs" qui contient des dictionnaires.
# Chaque dictionnaire a deux clés :
#  - "french" : la forme du verbe en français
#  - "forms"  : la liste des trois formes en anglais (base form, past simple,
#               past participle).
################################################################################
verbs = [
    {"french": "(se) réveiller", "forms": ["awake", "awoke", "awoken"]},
    {"french": "être", "forms": ["be", "was", "been"]},
    {"french": "supporter", "forms": ["bear", "bore", "borne"]},
    {"french": "battre", "forms": ["beat", "beat", "beaten"]},
    {"french": "devenir", "forms": ["become", "became", "become"]},
    {"french": "commencer", "forms": ["begin", "began", "begun"]},
    {"french": "(se) courber", "forms": ["bend", "bent", "bent"]},
    {"french": "parier", "forms": ["bet", "bet", "bet"]},
    {"french": "mordre", "forms": ["bite", "bit", "bitten"]},
    {"french": "saigner", "forms": ["bleed", "bled", "bled"]},
    {"french": "souffler", "forms": ["blow", "blew", "blown"]},
    {"french": "casser", "forms": ["break", "broke", "broken"]},
    {"french": "élever, donner naissance", "forms": ["breed", "bred", "bred"]},
    {"french": "apporter", "forms": ["bring", "brought", "brought"]},
    {"french": "transmettre", "forms": ["broadcast", "broadcast", "broadcast"]},
    {"french": "construire", "forms": ["build", "built", "built"]},
    {"french": "brûler", "forms": ["burn", "burnt", "burnt"]},
    {"french": "éclater", "forms": ["burst", "burst", "burst"]},
    {"french": "acheter", "forms": ["buy", "bought", "bought"]},
    {"french": "attraper", "forms": ["catch", "caught", "caught"]},
    {"french": "choisir", "forms": ["choose", "chose", "chosen"]},
    {"french": "venir", "forms": ["come", "came", "come"]},
    {"french": "coûter", "forms": ["cost", "cost", "cost"]},
    {"french": "couper", "forms": ["cut", "cut", "cut"]},
    {"french": "distribuer", "forms": ["deal", "dealt", "dealt"]},
    {"french": "creuser", "forms": ["dig", "dug", "dug"]},
    {"french": "faire", "forms": ["do", "did", "done"]},
    {"french": "dessiner", "forms": ["draw", "drew", "drawn"]},
    {"french": "rêver", "forms": ["dream", "dreamt", "dreamt"]},
    {"french": "boire", "forms": ["drink", "drank", "drunk"]},
    {"french": "conduire", "forms": ["drive", "drove", "driven"]},
    {"french": "manger", "forms": ["eat", "ate", "eaten"]},
    {"french": "tomber", "forms": ["fall", "fell", "fallen"]},
    {"french": "nourrir", "forms": ["feed", "fed", "fed"]},
    {"french": "sentir", "forms": ["feel", "felt", "felt"]},
    {"french": "combattre", "forms": ["fight", "fought", "fought"]},
    {"french": "trouver", "forms": ["find", "found", "found"]},
    {"french": "lancer", "forms": ["fling", "flung", "flung"]},
    {"french": "voler", "forms": ["fly", "flew", "flown"]},
    {"french": "interdire", "forms": ["forbid", "forbade", "forbidden"]},
    {"french": "oublier", "forms": ["forget", "forgot", "forgotten"]},
    {"french": "pardonner", "forms": ["forgive", "forgave", "forgiven"]},
    {"french": "geler", "forms": ["freeze", "froze", "frozen"]},
    {"french": "obtenir", "forms": ["get", "got", "got"]},
    {"french": "donner", "forms": ["give", "gave", "given"]},
    {"french": "aller", "forms": ["go", "went", "gone"]},
    {"french": "pousser", "forms": ["grow", "grew", "grown"]},
    {"french": "pendre", "forms": ["hang", "hung", "hung"]},
    {"french": "avoir", "forms": ["have", "had", "had"]},
    {"french": "entendre", "forms": ["hear", "heard", "heard"]},
    {"french": "cacher", "forms": ["hide", "hid", "hidden"]},
    {"french": "frapper", "forms": ["hit", "hit", "hit"]},
    {"french": "tenir", "forms": ["hold", "held", "held"]},
    {"french": "faire mal", "forms": ["hurt", "hurt", "hurt"]},
    {"french": "garder", "forms": ["keep", "kept", "kept"]},
    {"french": "s'agenouiller", "forms": ["kneel", "knelt", "knelt"]},
    {"french": "savoir", "forms": ["know", "knew", "known"]},
    {"french": "placer", "forms": ["lay", "laid", "laid"]},
    {"french": "conduire", "forms": ["lead", "led", "led"]},
    {"french": "s'appuyer", "forms": ["lean", "leant", "leant"]},
    {"french": "sauter", "forms": ["leap", "leapt", "leapt"]},
    {"french": "apprendre", "forms": ["learn", "learnt", "learnt"]},
    {"french": "quitter", "forms": ["leave", "left", "left"]},
    {"french": "prêter", "forms": ["lend", "lent", "lent"]},
    {"french": "laisser", "forms": ["let", "let", "let"]},
    {"french": "être couché", "forms": ["lie", "lay", "lain"]},
    {"french": "allumer", "forms": ["light", "lit", "lit"]},
    {"french": "perdre", "forms": ["lose", "lost", "lost"]},
    {"french": "faire", "forms": ["make", "made", "made"]},
    {"french": "vouloir dire", "forms": ["mean", "meant", "meant"]},
    {"french": "rencontrer", "forms": ["meet", "met", "met"]},
    {"french": "tondre", "forms": ["mow", "mowed", "mown"]},
    {"french": "payer", "forms": ["pay", "paid", "paid"]},
    {"french": "mettre", "forms": ["put", "put", "put"]},
    {"french": "lire", "forms": ["read", "read", "read"]},
    {"french": "aller (à cheval ou à bicyclette)", "forms": ["ride", "rode", "ridden"]},
    {"french": "sonner", "forms": ["ring", "rang", "rung"]},
    {"french": "se lever", "forms": ["rise", "rose", "risen"]},
    {"french": "courir", "forms": ["run", "ran", "run"]},
    {"french": "scier", "forms": ["saw", "sawed", "sawn"]},
    {"french": "dire", "forms": ["say", "said", "said"]},
    {"french": "voir", "forms": ["see", "saw", "seen"]},
    {"french": "vendre", "forms": ["sell", "sold", "sold"]},
    {"french": "envoyer", "forms": ["send", "sent", "sent"]},
    {"french": "placer", "forms": ["set", "set", "set"]},
    {"french": "coudre", "forms": ["sew", "sewed", "sewn"]},
    {"french": "secouer", "forms": ["shake", "shook", "shaken"]},
    {"french": "briller", "forms": ["shine", "shone", "shone"]},
    {"french": "tirer", "forms": ["shoot", "shot", "shot"]},
    {"french": "montrer", "forms": ["show", "showed", "shown"]},
    {"french": "fermer", "forms": ["shut", "shut", "shut"]},
    {"french": "chanter", "forms": ["sing", "sang", "sung"]},
    {"french": "(s') enfoncer", "forms": ["sink", "sank", "sunk"]},
    {"french": "(s') asseoir", "forms": ["sit", "sat", "sat"]},
    {"french": "dormir", "forms": ["sleep", "slept", "slept"]},
    {"french": "glisser", "forms": ["slide", "slid", "slid"]},
    {"french": "sentir", "forms": ["smell", "smelt", "smelt"]},
    {"french": "parler", "forms": ["speak", "spoke", "spoken"]},
    {"french": "se presser", "forms": ["speed", "sped", "sped"]},
    {"french": "épeler", "forms": ["spell", "spelt", "spelt"]},
    {"french": "dépenser", "forms": ["spend", "spent", "spent"]},
    {"french": "cracher", "forms": ["spit", "spat", "spat"]},
    {"french": "diviser", "forms": ["split", "split", "split"]},
    {"french": "abîmer, gâter", "forms": ["spoil", "spoilt", "spoilt"]},
    {"french": "étaler", "forms": ["spread", "spread", "spread"]},
    {"french": "jaillir", "forms": ["spring", "sprang", "sprung"]},
    {"french": "être debout", "forms": ["stand", "stood", "stood"]},
    {"french": "voler (dérober)", "forms": ["steal", "stole", "stolen"]},
    {"french": "coller", "forms": ["stick", "stuck", "stuck"]},
    {"french": "piquer", "forms": ["sting", "stung", "stung"]},
    {"french": "sentir mauvais", "forms": ["stink", "stank", "stunk"]},
    {"french": "marcher à grands pas", "forms": ["stride", "strode", "stridden"]},
    {"french": "frapper", "forms": ["strike", "struck", "struck"]},
    {"french": "jurer", "forms": ["swear", "swore", "sworn"]},
    {"french": "balayer", "forms": ["sweep", "swept", "swept"]},
    {"french": "nager", "forms": ["swim", "swam", "swum"]},
    {"french": "prendre", "forms": ["take", "took", "taken"]},
    {"french": "enseigner", "forms": ["teach", "taught", "taught"]},
    {"french": "déchirer", "forms": ["tear", "tore", "torn"]},
    {"french": "dire", "forms": ["tell", "told", "told"]},
    {"french": "penser", "forms": ["think", "thought", "thought"]},
    {"french": "jeter", "forms": ["throw", "threw", "thrown"]},
    {"french": "comprendre", "forms": ["understand", "understood", "understood"]},
    {"french": "troubler", "forms": ["upset", "upset", "upset"]},
    {"french": "réveiller", "forms": ["wake", "woke", "woken"]},
    {"french": "porter (un vêtement)", "forms": ["wear", "wore", "worn"]},
    {"french": "gagner", "forms": ["win", "won", "won"]},
    {"french": "écrire", "forms": ["write", "wrote", "written"]}
]

################################################################################
# On définit une classe "QuizHandler" qui hérite de "BaseHTTPRequestHandler".
# Cette classe va s'occuper de répondre aux requêtes HTTP GET et POST,
# tout en maintenant un état commun (index du verbe, score, etc.) pour
# toutes les sessions.
################################################################################
class QuizHandler(BaseHTTPRequestHandler):
    """
    Gère le quiz de verbes irréguliers en maintenant un état global
    via des variables de classe partagées.
    """

    # On définit des variables de *classe* (et non d'instance).
    # Cela signifie qu'elles sont partagées entre toutes les requêtes.
    current_index = 0   # Indique quel verbe (index) est en cours
    score = 0           # Indique combien de réponses correctes on a eues
    start_time = None   # Stocke l'heure (timestamp) de début du quiz
    time_records = []   # Liste pour garder l'historique des durées (temps total)

    def render_quiz_page(self, message=""):
        """
        Génère la page HTML pour poser la question suivante.
        """
        # On récupère le verbe français associé à l'index courant, 
        # en allant chercher la variable de classe via QuizHandler.
        verb_fr = verbs[QuizHandler.current_index]["french"]

        # On renvoie une chaîne de caractères qui contient le HTML du formulaire
        # + un message éventuel + le score actuel.
        # **On ne commente pas chaque ligne HTML** comme demandé.
        html_content = f"""
        <!DOCTYPE html>
        <html lang="fr">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Quiz de Verbes Irréguliers</title>
            <link rel="stylesheet" href="/styles.css">
        </head>
        <body>
            <h1>Quiz de Verbes Irréguliers</h1>
            <p><strong>Verbe en français :</strong> {verb_fr}</p>

            <form method="POST">
              <div class="form-row">
                <label for="form1">Bv </label>
                <input type="text" id="form1" name="form1" required>
              </div>
              <div class="form-row">
                <label for="form2">Past Simple</label>
                <input type="text" id="form2" name="form2" required>
              </div>
              <div class="form-row">
                <label for="form3">Past Participle</label>
                <input type="text" id="form3" name="form3" required>
              </div>
              <button type="submit">Soumettre</button>
            </form>

            <p style="color: red;">{message}</p>
            <p>Score actuel : {QuizHandler.score}/{len(verbs)}</p>
        </body>
        </html>
        """
        return html_content

    def render_results_page(self):
        """
        Génère la page finale (score + temps total + historique).
        """
        # On calcule le temps écoulé depuis le début.
        # On récupère l'heure actuelle avec time.time()
        end_time = time.time()

        # On fait la différence pour obtenir la durée.
        elapsed_time = end_time - QuizHandler.start_time

        # On ajoute cette durée dans la liste des time_records
        QuizHandler.time_records.append(elapsed_time)

        # On construit une portion HTML qui liste tous les temps enregistrés.
        historique_html = "".join(
            f"<li>{t:.2f} secondes</li>" 
            for t in QuizHandler.time_records
        )

        # On renvoie la page HTML finale (score + temps + historique).
        # **On ne commente pas chaque ligne du HTML**.
        html_content = f"""
        <!DOCTYPE html>
        <html lang="fr">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Quiz Terminé</title>
            <link rel="stylesheet" href="/styles.css">
        </head>
        <body>
            <h1>Quiz Terminé !</h1>
            <p>Votre score final est de {QuizHandler.score}/{len(verbs)}.</p>
            <p>Temps total : {elapsed_time:.2f} secondes.</p>

            <h2>Historique des performances :</h2>
            <ul>
                {historique_html}
            </ul>
        </body>
        </html>
        """
        return html_content

    def do_GET(self):
        """
        Gère les requêtes GET (affichage du quiz ou de la page finale).
        """
        # Cette portion vérifie si la requête demande le fichier CSS :
        if self.path == "/styles.css":
            try:
                # On envoie une réponse 200 (OK).
                self.send_response(200)
                # On précise que le type de contenu est du CSS.
                self.send_header("Content-type", "text/css")
                self.end_headers()
                # On ouvre "styles.css" et on l'écrit tel quel dans la réponse.
                with open("styles.css", "rb") as f:
                    self.wfile.write(f.read())
            except FileNotFoundError:
                # Si le fichier n'est pas trouvé, on renvoie une erreur 404.
                self.send_response(404)
                self.end_headers()
            return

        # On affiche un message de débogage qui montre l'index actuel.
        print(f"DEBUG: current_index dans do_GET = {QuizHandler.current_index}")

        # Si l'index est 0, cela signifie que c'est le début du quiz.
        # On initialise donc le chronomètre en stockant l'heure actuelle.
        if QuizHandler.current_index == 0:
            QuizHandler.start_time = time.time()

        # On regarde si on est encore dans la plage des verbes (index < nombre total).
        if QuizHandler.current_index < len(verbs):
            # On renvoie une réponse 200 (OK).
            self.send_response(200)
            # On indique que c'est du HTML.
            self.send_header("Content-type", "text/html")
            self.end_headers()
            # On construit la page de quiz grâce à render_quiz_page().
            page = self.render_quiz_page()
            # On envoie le code HTML au navigateur.
            self.wfile.write(page.encode("utf-8"))
        else:
            # Sinon, si tous les verbes ont été traités, on affiche la page finale.
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            page = self.render_results_page()
            self.wfile.write(page.encode("utf-8"))

    def do_POST(self):
        """
        Gère la soumission du formulaire (réponses de l’utilisateur).
        """
        # On récupère la taille des données envoyées via POST.
        content_length = int(self.headers.get('Content-Length', 0))
        # On lit exactement ce nombre d'octets depuis la requête.
        post_data = self.rfile.read(content_length)
        # On décode l'ensemble et on parse les paramètres envoyés (champ1, champ2...).
        form_data = urllib.parse.parse_qs(post_data.decode())

        # On va chercher la valeur de "form1", "form2", "form3" dans form_data,
        # et on passe tout en minuscules, en enlevant les espaces aux extrémités.
        user_answers = [
            form_data.get("form1", [""])[0].strip().lower(),
            form_data.get("form2", [""])[0].strip().lower(),
            form_data.get("form3", [""])[0].strip().lower(),
        ]

        # On récupère les trois formes correctes du verbe actuel,
        # qu'on passe également en minuscules, sans espaces.
        correct_answers = [
            form.lower().strip()
            for form in verbs[QuizHandler.current_index]["forms"]
        ]

        # On affiche pour le débogage la réponse de l'utilisateur et la bonne réponse.
        print(f"DEBUG: user_answers = {user_answers}")
        print(f"DEBUG: correct_answers = {correct_answers}")
        print(f"DEBUG: current_index (avant incrément) = {QuizHandler.current_index}")

        # On compare si toutes les formes (base, past simple, past participle)
        # correspondent aux réponses correctes.
        if all(
            user_answer == correct_answer
            for user_answer, correct_answer in zip(user_answers, correct_answers)
        ):
            # Si c'est correct, on incrémente le score de 1.
            QuizHandler.score += 1
            message = "Correct !"
        else:
            # Sinon, on affiche la "bonne" solution entre guillemets.
            solutions = ", ".join(verbs[QuizHandler.current_index]["forms"])
            message = f"Incorrect. Les bonnes réponses sont : {solutions}"

        # On passe au verbe suivant, en incrémentant l'index.
        QuizHandler.current_index += 1
        print(f"DEBUG: current_index (après incrément) = {QuizHandler.current_index}")

        # Si l'index est encore plus petit que le nombre de verbes,
        # on continue le quiz, sinon on affiche la page finale (via do_GET()).
        if QuizHandler.current_index < len(verbs):
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            page = self.render_quiz_page(message)
            self.wfile.write(page.encode("utf-8"))
        else:
            # Appeler do_GET() nous permet d'afficher la page de résultats
            # lorsqu'on a épuisé la liste des verbes.
            self.do_GET()

################################################################################
# On définit une fonction run() qui va démarrer le serveur HTTP sur le port 8080.
# Puis on attend indéfiniment les requêtes.
################################################################################
def run():
    """
    Lance le serveur sur le port 8080.
    Accès via http://localhost:8080
    """
    # On indique comme adresse serveur : '', ce qui signifie "toutes les interfaces",
    # et on écoute le port 8080.
    server_address = ('', 8080)

    # On crée une instance de HTTPServer, en lui passant l'adresse et la classe
    # qui va gérer les requêtes (QuizHandler).
    httpd = HTTPServer(server_address, QuizHandler)

    # On affiche un message pour dire que le serveur est lancé.
    print("Serveur lancé sur http://localhost:8080 ...")

    # On entre dans une boucle infinie pour écouter les requêtes et y répondre.
    httpd.serve_forever()


# Point d'entrée principal : si ce fichier est lancé directement,
# on appelle la fonction run() pour démarrer le serveur.
if __name__ == "__main__":
    run()
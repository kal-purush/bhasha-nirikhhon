import tkinter as tk
from tkinter import filedialog, messagebox
import pyperclip
import pyautogui
import time
import os
import shutil
import pygetwindow as gw
import re
import string
import sys
from pydub import AudioSegment
from pydub.utils import which

AudioSegment.converter = which("ffmpeg")

# Message initial et avertissement
message_initial = """Colle ton texte ici formaté avec des sauts de ligne après chaque point.
Assure-toi d'avoir Google Chrome connecté à ton compte et l'application ElevenLabs ouverte sur la bonne page.
Assure-toi aussi que tous tes réglages sont faits dans ElevenLabs.
Ne touche pas à l'ordinateur pendant l'exécution du script."""

message_avertissement = "Assure-toi que l'onglet ElevenLabs est ouvert dans Google Chrome, et qu'aucune autre fenêtre ne viendra interférer."

destination_finale = None  # Variable globale pour le dossier de destination final
fichiers_mp3_crees = []    # Liste des fichiers MP3 créés
lignes_globales = []       # Liste globale des lignes traitées

# Fonction pour effacer les mp3 commençant par 'ElevenLab' et tous les fichiers .sfk dans Downloads
def effacer_mp3_elevenlabs():
    chemin_source = os.path.expanduser("~\\Downloads")
    
    # Liste des fichiers MP3 commençant par 'ElevenLab' et tous les fichiers avec l'extension .sfk
    fichiers_a_effacer = [
        f for f in os.listdir(chemin_source)
        if (f.startswith('ElevenLab') and f.endswith('.mp3')) or f.endswith('.sfk')
    ]
    
    if fichiers_a_effacer:
        reponse = messagebox.askyesno(
            "Effacer les fichiers",
            "Voulez-vous effacer tous les fichiers MP3 commençant par 'ElevenLab' dans le dossier Downloads, ainsi que tous les fichiers avec l'extension .sfk ?"
        )
        if reponse:
            for fichier in fichiers_a_effacer:
                try:
                    os.remove(os.path.join(chemin_source, fichier))
                except Exception as e:
                    print(f"Erreur lors de la suppression du fichier {fichier}: {e}")
            messagebox.showinfo("Suppression terminée", "Les fichiers MP3 et .sfk ont été supprimés.")
        else:
            messagebox.showinfo("Info", "Les fichiers MP3 et .sfk n'ont pas été supprimés.")
    else:
        messagebox.showinfo("Info", "Aucun fichier MP3 commençant par 'ElevenLab' ni de fichier .sfk trouvé dans le dossier Downloads.")

# Fonction pour nettoyer les noms de fichiers
def nettoyer_nom_fichier(nom):
    # Remplacer les caractères non autorisés par '_'
    nom_nettoye = re.sub(r'[<>:"/\\|?*]', '_', nom)
    # Supprimer les caractères non imprimables
    nom_nettoye = ''.join(c for c in nom_nettoye if c in string.printable)
    # Limiter la longueur du nom
    return nom_nettoye[:255]

# Fonction pour copier tout le texte de l'encart
def copier_texte():
    texte = texte_encart.get("1.0", "end-1c")  # Obtenir tout le texte de l'encart
    lignes = [ligne.strip() for ligne in texte.split('\n') if ligne.strip()]
    lignes_nettoyees = []
    for ligne in lignes:
        # Retirer les parenthèses et leur contenu du texte à copier
        ligne_sans_parentheses = re.sub(r'\s*\(.*?\)\s*', '', ligne).strip()
        # Insérer la phrase "inspirez et expirez, détendu. " avant chaque ligne
        ligne_modifiee = "inspirez et expirez, détendu. " + ligne_sans_parentheses
        # Trouver tous les nombres à l'intérieur des parenthèses
        nombres = re.findall(r'\(([^()]*)\)', ligne)
        temps_silence = 0
        for contenu in nombres:
            nombres_dans_contenu = re.findall(r'\d+', contenu)
            temps_silence += sum(int(n) for n in nombres_dans_contenu)
        lignes_nettoyees.append((ligne_modifiee, temps_silence))
    return lignes_nettoyees

# Fonction pour charger le texte depuis un fichier .txt
def charger_texte_fichier():
    fichier_txt = filedialog.askopenfilename(title="Sélectionner un fichier texte", filetypes=[("Fichiers texte", "*.txt")])
    if fichier_txt:
        with open(fichier_txt, 'r', encoding='utf-8') as f:
            contenu = f.read()
            texte_encart.delete("1.0", tk.END)
            texte_encart.insert(tk.END, contenu)

# Fonction pour activer la fenêtre Chrome avec ElevenLabs
def activer_fenetre_chrome_elevenlabs():
    # Rechercher les fenêtres dont le titre contient 'ElevenLabs' et 'Google Chrome'
    all_windows = gw.getAllWindows()
    for window in all_windows:
        if 'ElevenLabs' in window.title and 'Google Chrome' in window.title:
            try:
                if window.isMinimized:
                    window.restore()
                window.activate()
                window.maximize()  # S'assurer que la fenêtre est maximisée
                time.sleep(1)  # Attendre pour que la fenêtre soit bien activée
                return True
            except Exception as e:
                print(f"Erreur lors de l'activation de la fenêtre Chrome ElevenLabs : {e}")
                return False
    print("Fenêtre 'ElevenLabs' dans Google Chrome non trouvée.")
    return False

# Note: OpenCV doit être installé pour utiliser le paramètre 'confidence' dans pyautogui.locateOnScreen
# Pour installer OpenCV, exécutez : pip install opencv-python

# Fonction pour détecter le message d'erreur
def detect_error_message():
    # Assumes that 'error_message.png' is an image of the error message
    try:
        error_location = pyautogui.locateOnScreen('error_message.png', confidence=0.8)
        return error_location is not None
    except Exception as e:
        print(f"Erreur lors de la détection du message d'erreur: {e}")
        return False

# Fonction pour naviguer vers ElevenLabs et effectuer les actions
def naviguer_vers_elev(ligne):
    if not activer_fenetre_chrome_elevenlabs():
        return False  # Si la fenêtre n'est pas trouvée, on arrête la fonction et indique un échec

    # Copier et coller la ligne actuelle
    pyperclip.copy(ligne)  # Copier la ligne actuelle dans le presse-papiers
    pyautogui.click(x=945, y=459)  # Clic sur le champ de saisie de texte (coordonnées absolues)
    time.sleep(0.5)  # Petite pause pour stabiliser le clic

    pyautogui.hotkey('ctrl', 'a')  # Sélectionner tout le texte
    time.sleep(0.2)  # Pause entre les actions
    pyautogui.hotkey('ctrl', 'v')  # Coller le texte copié
    time.sleep(0.2)  # Pause entre les actions
    pyautogui.hotkey('shift', 'enter')  # Lancer la génération du mp3

    # Ajuster le temps de pause selon la longueur de la ligne
    if len(ligne) > 50:
        temps_attente = max(5.0, len(ligne) * 0.07)
    else:
        temps_attente = max(3.5, len(ligne) * 0.05)

    time.sleep(temps_attente)  # Attendre que la génération soit terminée

    # Essayer de télécharger le MP3, avec des tentatives en cas d'erreur
    max_attempts = 1000
    attempts = 0
    while attempts < max_attempts:
        # Clic pour lancer le téléchargement du MP3
        pyautogui.click(x=1809, y=957)  # Clic sur le bouton de téléchargement (coordonnées absolues)
        time.sleep(1)  # Attendre un peu pour que le message d'erreur puisse apparaître

        # Vérifier si le message d'erreur apparaît
        if detect_error_message():
            attempts += 1
            print(f"Tentative {attempts}/{max_attempts} échouée. Le serveur est occupé.")
            if attempts >= max_attempts:
                print("Le serveur est occupé. Nombre maximal de tentatives atteint. Passage à l'étape de mixage.")
                return False  # Indiquer un échec pour cette phrase
            else:
                time.sleep(3)  # Attendre 3 secondes avant de réessayer le même clic
                continue  # Réessayer avec la même phrase
        else:
            break  # Pas d'erreur, on sort de la boucle et passe à l'étape suivante normalement

    pyautogui.click(x=945, y=459)  # Pour recentrer l'attention sur la fenêtre d'entrée ElevenLabs.
    return True  # Indiquer un succès

# Fonction principale exécutée lors du clic sur "Run Task"
def run_task():
    global lignes_globales, fichiers_mp3_crees
    chemin_source = os.path.expanduser("~\\Downloads")
    fichiers_mp3_initial = set(f for f in os.listdir(chemin_source) if f.endswith(".mp3"))

    lignes = copier_texte()  # Obtenir les lignes non vides du texte avec les silences
    lignes_globales = lignes  # Stocker globalement pour d'autres fonctions

    compteur_telechargements = 0  # Initialiser un compteur pour les téléchargements
    stop_processing = False  # Indicateur pour arrêter le traitement

    for i, (ligne_modifiee, temps_silence) in enumerate(lignes):
        success = naviguer_vers_elev(ligne_modifiee)
        if not success:
            print("Arrêt du traitement en raison d'erreurs répétées.")
            stop_processing = True
            break  # Sortir de la boucle des lignes

        time.sleep(temps_silence)  # Attendre le silence spécifié

        compteur_telechargements += 1  # Incrémenter le compteur après chaque téléchargement

        # Pause de 10 secondes tous les quatre téléchargements
        if compteur_telechargements % 4 == 0:
            time.sleep(10)

    time.sleep(5)  # Attendre que tous les fichiers soient bien téléchargés

    # Obtenir les nouveaux fichiers MP3
    fichiers_mp3_final = set(f for f in os.listdir(chemin_source) if f.endswith(".mp3"))
    nouveaux_fichiers_mp3 = list(fichiers_mp3_final - fichiers_mp3_initial)

    # Si le traitement a été arrêté prématurément, adapter la liste des lignes pour le reste du programme
    if stop_processing:
        lignes = lignes[:compteur_telechargements]
        lignes_globales = lignes

    attribuer_noms_fichiers_mp3(lignes, nouveaux_fichiers_mp3, chemin_source)
    proposer_deplacer_fichiers(lignes)

# Fonction pour attribuer les noms des fichiers .mp3
def attribuer_noms_fichiers_mp3(lignes, nouveaux_fichiers_mp3, chemin_source):
    global fichiers_mp3_crees
    if len(nouveaux_fichiers_mp3) == 0:
        messagebox.showwarning("Attention", "Aucun fichier MP3 n'a été téléchargé.")
        return

    # Trier les nouveaux fichiers par date de création
    nouveaux_fichiers_mp3.sort(key=lambda f: os.path.getctime(os.path.join(chemin_source, f)))

    fichiers_crees = []
    for i, fichier in enumerate(nouveaux_fichiers_mp3):
        if i < len(lignes):
            ligne_modifiee, _ = lignes[i]
            # Utiliser un index pour assurer l'unicité du nom de fichier
            nouveau_nom = f"{i+1:03d}_{nettoyer_nom_fichier(ligne_modifiee)}.mp3"
            ancien_chemin = os.path.join(chemin_source, fichier)
            nouveau_chemin = os.path.join(chemin_source, nouveau_nom)
            try:
                os.rename(ancien_chemin, nouveau_chemin)
                fichiers_crees.append(nouveau_nom)
            except Exception as e:
                print(f"Erreur lors du renommage du fichier {fichier}: {e}")
    fichiers_mp3_crees = fichiers_crees  # Stocker la liste des fichiers créés

# Fonction pour proposer de déplacer les fichiers .mp3
def proposer_deplacer_fichiers(lignes):
    global destination_finale

    def move_files():
        chemin_source = os.path.expanduser("~\\Downloads")
        for fichier in fichiers_mp3_crees:
            try:
                shutil.move(os.path.join(chemin_source, fichier), os.path.join(destination_finale, fichier))
            except Exception as e:
                print(f"Erreur lors du déplacement du fichier {fichier}: {e}")

        messagebox.showinfo("Fichiers déplacés",
                            f"Vos fichiers .mp3 ont été déplacés vers {destination_finale}")
        generer_fichier_silences(lignes)
        afficher_options_finales()

    reponse = messagebox.askyesno("Déplacer les fichiers",
                                  "Voulez-vous déplacer les fichiers .mp3 du dossier 'Downloads' vers un autre emplacement ?")
    if reponse:
        destination_finale = filedialog.askdirectory()
        if destination_finale:
            # Vérifier s'il y a déjà des fichiers mp3 dans le dossier cible
            existing_mp3_files = [f for f in os.listdir(destination_finale) if f.endswith('.mp3')]
            if existing_mp3_files:
                def open_folder_to_clean():
                    os.startfile(destination_finale)

                def delete_old_mp3s():
                    for f in existing_mp3_files:
                        try:
                            os.remove(os.path.join(destination_finale, f))
                        except Exception as e:
                            print(f"Erreur lors de la suppression du fichier {f}: {e}")
                    messagebox.showinfo("Suppression terminée", "Les anciens fichiers mp3 ont été supprimés.")
                    move_files()
                    options_window.destroy()

                options_window = tk.Toplevel(root)
                options_window.title("Anciens mp3 détectés")
                options_window.geometry("500x200")
                label_msg = tk.Label(options_window, text="Oops! il reste des mp3 dans le dossier cible. Mieux vaut les effacer pour partir sur une base saine.")
                label_msg.pack(padx=20, pady=10)

                btn_open_folder = tk.Button(options_window, text="Ouvrir le dossier pour faire le ménage moi-même sans que ça perturbe python", command=open_folder_to_clean)
                btn_open_folder.pack(padx=20, pady=6)

                btn_delete_old_mp3s = tk.Button(options_window, text="Supprimer les anciens mp3 dans le dossier cible", command=delete_old_mp3s)
                btn_delete_old_mp3s.pack(padx=20, pady=6)

                # Empêcher l'exécution ultérieure jusqu'à ce que l'utilisateur résolve cela
                return
            else:
                # Pas de fichiers mp3 existants, procéder au déplacement
                move_files()
        else:
            messagebox.showwarning("Annulation", "Aucun dossier sélectionné. Opération annulée.")
            return
    else:
        # L'utilisateur a choisi de ne pas déplacer les fichiers
        pass

# Fonction pour générer un fichier texte avec les silences associés
def generer_fichier_silences(lignes):
    if destination_finale:
        chemin_fichier_silences = os.path.join(destination_finale, "silences_associes.txt")
        with open(chemin_fichier_silences, 'w', encoding='utf-8') as f:
            for ligne_modifiee, temps_silence in lignes:
                if temps_silence > 0:
                    f.write(f"{ligne_modifiee}... ({temps_silence} secondes de silence)\n")
                else:
                    f.write(f"{ligne_modifiee}\n")
        messagebox.showinfo("Fichier créé", "Le fichier de confirmation des silences a été généré.")

# Fonction pour afficher les options finales
def afficher_options_finales():
    options_fenetre = tk.Toplevel(root)
    options_fenetre.title("Actions finales")
    options_fenetre.geometry("500x300")
    options_fenetre.attributes("-topmost", True)

    bouton_ouvrir_sans_quitter = tk.Button(options_fenetre, text="Ouvrir le dossier sans quitter", command=ouvrir_dossier_sans_quitter)
    bouton_ouvrir_sans_quitter.pack(padx=20, pady=10)

    bouton_ouvrir = tk.Button(options_fenetre, text="Ouvrir le dossier et quitter", command=ouvrir_dossier_et_quitter)
    bouton_ouvrir.pack(padx=20, pady=10)

    bouton_effacer = tk.Button(options_fenetre, text="Effacer les MP3 créés et quitter après avoir ouvert le dossier", command=effacer_tous_les_mp3_et_quitter)
    bouton_effacer.pack(padx=20, pady=10)

    bouton_mix = tk.Button(options_fenetre, text="Passer à la partie mix", command=passer_a_partie_mix)
    bouton_mix.pack(padx=20, pady=10)

# Fonction pour ouvrir le dossier de destination et quitter
def ouvrir_dossier_et_quitter():
    if destination_finale:
        os.startfile(destination_finale)  # Ouvrir le dossier dans l'explorateur Windows
    root.quit()  # Quitter l'application

# Fonction pour ouvrir le dossier sans quitter
def ouvrir_dossier_sans_quitter():
    if destination_finale:
        os.startfile(destination_finale)  # Ouvrir le dossier dans l'explorateur Windows

# Fonction pour effacer les MP3 créés et quitter après avoir ouvert le dossier
def effacer_tous_les_mp3_et_quitter():
    if destination_finale:
        chemin_source = os.path.expanduser("~\\Downloads")
        # Ouvrir le dossier
        os.startfile(destination_finale)
        time.sleep(2)  # Attendre que le dossier s'ouvre
        # Supprimer les fichiers créés
        for fichier in fichiers_mp3_crees:
            fichier_source = os.path.join(chemin_source, fichier)
            if os.path.exists(fichier_source):
                try:
                    os.remove(fichier_source)
                except Exception as e:
                    print(f"Erreur lors de la suppression du fichier {fichier_source}: {e}")
            fichier_dest = os.path.join(destination_finale, fichier)
            if os.path.exists(fichier_dest):
                try:
                    os.remove(fichier_dest)
                except Exception as e:
                    print(f"Erreur lors de la suppression du fichier {fichier_dest}: {e}")

        messagebox.showinfo("Nettoyage", "Les fichiers .mp3 créés ont été effacés.")
        root.quit()  # Quitter l'application

# Fonction pour ajouter les silences aux fichiers MP3
def passer_a_partie_mix():
    reponse = messagebox.askyesno("Mixage", "Voulez-vous remplacer les MP3 existants par leur version avec silence ajouté à la fin ?")
    if reponse:
        chemin_dossier = destination_finale if destination_finale else os.path.expanduser("~\\Downloads")
        modifier_mp3_avec_silence(chemin_dossier, remplacer=True)
    else:
        reponse_nouveau = messagebox.askyesno("Mixage", "Voulez-vous créer de nouvelles versions MP3 avec silence ajouté à la fin dans un nouveau dossier ?")
        if reponse_nouveau:
            dossier_nouveau = filedialog.askdirectory(title="Sélectionnez le dossier pour les nouveaux fichiers MP3")
            if dossier_nouveau:
                chemin_dossier = destination_finale if destination_finale else os.path.expanduser("~\\Downloads")
                modifier_mp3_avec_silence(chemin_dossier, remplacer=False, dossier_nouveau=dossier_nouveau)
            else:
                messagebox.showwarning("Annulation", "Aucun dossier sélectionné. Opération annulée.")
                return
        else:
            messagebox.showinfo("Annulation", "Aucune modification apportée aux fichiers MP3.")
            return

    # Demander si l'utilisateur souhaite concaténer les mp3
    reponse_concat = messagebox.askyesno("Concaténation", "Voulez-vous concaténer les mp3 du plus ancien au plus récent?")
    if reponse_concat:
        # Effectuer la concaténation
        chemin_dossier = destination_finale if destination_finale else os.path.expanduser("~\\Downloads")
        concatenation_mp3(chemin_dossier)
        messagebox.showinfo("Concaténation terminée", "mp3 concaténés. Ouvrir le dossier pour voir le résultat.")
        ouvrir_dossier_sans_quitter()
    root.quit()  # Quitter l'application

# Fonction pour modifier les MP3 en ajoutant le silence
def modifier_mp3_avec_silence(chemin_dossier, remplacer=True, dossier_nouveau=None):
    for i, (ligne_modifiee, temps_silence) in enumerate(lignes_globales):
        if i < len(fichiers_mp3_crees):
            fichier_mp3 = fichiers_mp3_crees[i]
            chemin_fichier = os.path.join(chemin_dossier, fichier_mp3)
            try:
                audio = AudioSegment.from_mp3(chemin_fichier)
                # Ajouter le silence à la fin si temps_silence > 0
                if temps_silence > 0:
                    silence_audio = AudioSegment.silent(duration=temps_silence * 1000)  # Convertir en millisecondes
                    audio += silence_audio
                if remplacer:
                    audio.export(chemin_fichier, format="mp3")
                else:
                    nouveau_nom = f"{os.path.splitext(fichier_mp3)[0]}_silence.mp3"
                    chemin_nouveau = os.path.join(dossier_nouveau, nouveau_nom)
                    audio.export(chemin_nouveau, format="mp3")
            except Exception as e:
                print(f"Erreur lors de la modification du fichier {fichier_mp3}: {e}")

# Fonction pour concaténer les MP3
def concatenation_mp3(chemin_dossier):
    # Utiliser la liste des fichiers créés pour assurer le bon ordre
    mp3_files_full_paths = [os.path.join(chemin_dossier, f) for f in fichiers_mp3_crees]
    # Vérifier que les fichiers existent
    mp3_files_full_paths = [f for f in mp3_files_full_paths if os.path.exists(f)]
    # Charger et concaténer les fichiers mp3
    combined = AudioSegment.empty()
    for mp3_file in mp3_files_full_paths:
        audio = AudioSegment.from_mp3(mp3_file)
        combined += audio
    # Exporter le mp3 combiné
    output_file = os.path.join(chemin_dossier, '0concatenation.mp3')
    combined.export(output_file, format='mp3')

# Fonction pour créer la fenêtre d'arrêt d'urgence
def fenetre_arret_urgence():
    arret_fenetre = tk.Toplevel()
    arret_fenetre.title("Arrêt d'Urgence")
    arret_fenetre.geometry("300x150")
    arret_fenetre.attributes("-topmost", True)
    arret_fenetre.protocol("WM_DELETE_WINDOW", lambda: None)  # Désactiver la fermeture de la fenêtre

    label_alerte = tk.Label(arret_fenetre, text="ABSOLUMENT TOUT ARRÊTER", font=("Arial", 14, "bold"), fg="red")
    label_alerte.pack(padx=20, pady=20)

    bouton_arret = tk.Button(arret_fenetre, text="ARRÊT D'URGENCE", font=("Arial", 12), bg="red", fg="white", command=arreter_programme)
    bouton_arret.pack(padx=20, pady=10)

# Fonction pour arrêter le programme de manière propre
def arreter_programme():
    sys.exit(1)  # Terminer le programme proprement

# Fonction pour initialiser l'interface principale
def initialiser_interface():
    # Ajouter le message initial en haut de la fenêtre
    label_message = tk.Label(root, text=message_initial, padx=10, pady=10, justify="left")
    label_message.pack(padx=20, pady=10)

    # Ajouter un encart pour le texte
    global texte_encart
    texte_encart = tk.Text(root, wrap="word", height=20, width=80, padx=10, pady=10)
    texte_encart.pack(padx=20, pady=20)

    # Ajouter un bouton pour charger un fichier .txt
    bouton_charger_fichier = tk.Button(root, text="Je préfère soumettre un fichier .txt", command=charger_texte_fichier)
    bouton_charger_fichier.pack(padx=20, pady=5)

    # Ajouter le bouton "Run Task"
    bouton_run = tk.Button(root, text="Run Task", command=run_task)
    bouton_run.pack(padx=20, pady=10)

# Fonction pour continuer après la vérification
def continuer():
    avertissement_label.pack_forget()  # Masquer l'avertissement
    bouton_continuer.pack_forget()     # Masquer le bouton de confirmation
    effacer_mp3_elevenlabs()           # Appeler la fonction pour effacer les MP3 et .sfk si nécessaire
    initialiser_interface()            # Initialiser l'interface principale

# Création de la fenêtre principale
root = tk.Tk()
root.title("Instructions")

# Afficher le message d'avertissement en haut de la fenêtre
avertissement_label = tk.Label(root, text=message_avertissement, padx=10, pady=20, font=("Arial", 16, "bold"), fg="red")
avertissement_label.pack(padx=20, pady=20)

# Ajouter le bouton de confirmation
bouton_continuer = tk.Button(root, text="J'ai vérifié, je veux continuer", command=continuer, font=("Arial", 12), bg="green", fg="white")
bouton_continuer.pack(padx=20, pady=10)

# Créer la fenêtre d'arrêt d'urgence
fenetre_arret_urgence()

# Lancer la boucle principale Tkinter
root.mainloop()
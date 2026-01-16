function afficherFormulaire() {
    document.getElementById("formulaire").classList.remove("hidden");
    document.getElementById("tableau").classList.add("hidden");
    document.getElementById("tableau2").classList.add("hidden");
    document.getElementById("popupForm").classList.add("hidden");
  }
  
  function afficherTableauModifier() {
    document.getElementById("tableau").classList.remove("hidden");
    document.getElementById("tableau2").classList.add("hidden");
    document.getElementById("formulaire").classList.add("hidden");
    document.getElementById("popupForm").classList.add("hidden");
  }
  
  function afficherTableauSupprimer() {
    document.getElementById("tableau2").classList.remove("hidden");
    document.getElementById("tableau").classList.add("hidden");
    document.getElementById("formulaire").classList.add("hidden");
    document.getElementById("popupForm").classList.add("hidden");
  }
  
  function afficherPopup() {
    var popup = document.getElementById("popupForm");
    if (popup) {
      popup.classList.remove("hidden");
      overlay.classList.remove("hidden");
    } else {
      console.error("Erreur : Élément 'popupForm' non trouvé dans le DOM !");
    }
  }
  
  function fermerPopup() {
    var popup = document.getElementById("popupForm");
    if (popup) {
      popup.classList.add("hidden");
      overlay.classList.add("hidden");
    } else {
      console.error("Erreur : Élément 'popupForm' non trouvé dans le DOM !");
    }
  }
  
  // Variable globale pour stocker l'image sélectionnée
  var nouvelleImage;
  
  function afficherFormulaireModifier(id, nom, image, petiteDescription, description) {
    var form = document.createElement("form");
    form.innerHTML = `
        <div class="card">
            <div class="card-header">
                Formulaire de modification
            </div>
            <div class="card-body">
                <div class="form-group">
                    <label for="titreMachine">Titre de la machine:</label>
                    <input type="text" class="form-control" id="titreMachine" name="titre" value="${escapeHTML(
                      nom
                    )}" required>
                </div>
                <div class="form-group">
                    <label for="nouvelleImage">Nouvelle Image:</label>
                    <input type="file" id="image" name="image" accept="image/*">
                </div>
                <div class="form-group">
                    <label for="nouvellepetiteDescription">Nouvelle Petite Description:</label>
                    <textarea class="form-control" id="nouvellepetiteDescription" name="petiteDescription" rows="4" required>${escapeHTML(
                      petiteDescription
                    )}</textarea>
                </div>
                <div class="form-group">
                    <label for="nouvelleDescription">Nouvelle Description:</label>
                    <textarea class="form-control" id="nouvelleDescription" name="description" rows="4" required>${escapeHTML(
                      description
                    )}</textarea>
                </div>
                <input type="hidden" id="id_machine" name="id_machine" value="${escapeHTML(
                  id
                )}">
                <button type="button" class="btn btn-primary" onclick="sauvegarderModification()">Sauvegarder</button>
                <button type="button" class="btn btn-danger" onclick="fermerPopup()">Fermer</button>
            </div>
        </div>
    `;
  
    var popupContent = document.querySelector(".popup-content");
    if (popupContent) {
      popupContent.innerHTML = ""; // Nettoyer le contenu précédent
      popupContent.appendChild(form); // Ajouter le formulaire à la popup
      afficherPopup(); // Afficher la popup
      console.log("Formulaire de modification généré avec succès !");
  
      // Attachons un gestionnaire d'événements change à l'élément input de type fichier une fois qu'il est ajouté au DOM
      var inputImage = form.querySelector("#image");
      inputImage.addEventListener("change", function (event) {
        nouvelleImage = event.target.files[0];
        console.log("Fichier sélectionné :", nouvelleImage);
      });
    } else {
      console.error("Erreur : Conteneur de la popup non trouvé !");
    }
  }
  
  function sauvegarderModification() {
    var id_machine = document.getElementById("id_machine").value;
    var nouveauTitre = document.getElementById("titreMachine").value;
  
    // Récupérer la nouvelle description
    var nouvellepetiteDescription = document.getElementById(
      "nouvellepetiteDescription"
    ).value;
  
    var nouvelleDescription = document.getElementById(
      "nouvelleDescription"
    ).value;
  
    // Créer un objet FormData et ajouter les données du formulaire
    var formData = new FormData();
    formData.append("id_machine", id_machine);
    formData.append("nouveauTitre", nouveauTitre);
    formData.append("nouvellepetiteDescription", nouvellepetiteDescription);
    formData.append("nouvelleDescription", nouvelleDescription);
  
    // Vérifier si une image a été sélectionnée
    if (nouvelleImage) {
      console.log("Fichier sélectionné :", nouvelleImage);
      formData.append("image", nouvelleImage);
    } else {
      console.log("Aucun fichier sélectionné");
    }
  
    // Envoyer les données du formulaire via XMLHttpRequest
    var xhr = new XMLHttpRequest();
    xhr.open("POST", "../Serveur/modifier_machine.php");
    xhr.onload = function () {
      if (xhr.status === 200) {
        console.log("Modification réussie");
        // Recharger la page après la modification
        window.location.reload();
      } else {
        console.log("Erreur lors de la modification");
      }
    };
    xhr.send(formData); // Envoyer les données du formulaire avec FormData
  }
  
  // Fonction pour supprimer une machine
  function supprimerMachine(machineID) {
    if (confirm("Êtes-vous sûr de vouloir supprimer cette machine ?")) {
      var xhr = new XMLHttpRequest();
      xhr.open("POST", "../Serveur/supprimermachine.php", true);
      xhr.setRequestHeader("Content-Type", "application/x-www-form-urlencoded");
      xhr.onreadystatechange = function () {
        if (xhr.readyState === XMLHttpRequest.DONE) {
          if (xhr.status === 200) {
            // Actualiser la page après la suppression
            window.location.reload();
          } else {
            console.error(xhr.responseText);
          }
        }
      };
      xhr.send("machineID=" + machineID);
    }
  }
  
  // Fonction pour échapper les caractères HTML
  function escapeHTML(html) {
    return html
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;");
  }
  
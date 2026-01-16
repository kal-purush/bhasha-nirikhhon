// Suppression des éléments en AJAX

document.querySelectorAll('[data-delete]').forEach(a => { // Selection tout les lien qui on un attribut 'data-delete' et on boucle sur chaque liens
    a.addEventListener('click', e => { // lorsqu'on click sur un des liens...
        e.preventDefault() // suppression de l'event par défaut (le click normal)

        // Requête AJAX via la méthode "fetch()" qui permet de faire des requêtes/response en AJAX (fetch() possède des option: method, headers, body...)
        fetch(a.getAttribute('href'), { // On va chercher dans les liens l'attribut "href" 
            method: 'DELETE', // On donne la méthode delete comme méthode de requête
            headers: { // On ajoute des en-têtes (headers) à notre requête :
                'X-Requested-With': 'XMLHttpResquest', // en-tête qui permet de signifier qu'on est en AJAX
                'Content-Type' : 'application/json' // contenu en json
            },
            body: JSON.stringify({'_token': a.dataset.token}) // body est le corp de la requête et JSON.stringify() convertit une valeur javascript en chaîne JSON, ici le token du lien dataset
        // On récup la promesse (réponse) de "fetch()" et on lui applique la méthode json() pour récup la reponse en JSON 
        }).then(response => response.json()) // json() renvoi une promesse
            .then(data => { // de la réponse on extrait les données "data"
                if(data.success){ // Si la réponse est positive "success" alors...
                    // SUPPRESSION DE L'IMAGE
                    a.parentNode.parentNode.removeChild(a.parentNode); // Suppression de l'élément parent à notre lien
                }else{
                    alert(data.error) // Sinon la reponse est négative "error" alors on fait une alerte
                }
            })
            .catch(e => alert(e))
    })
})
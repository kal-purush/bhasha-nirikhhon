/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/*
Dans le fichier lib/affiche.js
- créer un tableau "projet" avec 3 clefs décrivant les informations suivantes:
	["nom"]    = "BDPhilia";
	["auteur"] = "votre nom";
	["copy"]   = "&copy; 2009 votre entreprise";
- créer une fonction affichePageBandeauHaut() qui affiche le bandeau du haut (<div id="bandeau">)
*/

/* global pagesDisponibles */

var projet = new Array();
projet["nom"]    = "BDPhilia";
projet["auteur"] = "DE COLO";
projet["copy"]   = "&copy; 2009 Natixis";

function affichePageBandeauHaut()
{
    document.write("<div id=\"bandeau\">&nbsp;</div>");
};

/*
  Dans le fichier lib/affiche.js
- créer une fonction affichePageTitre() qui affiche le titre (<div id="titre">) comme précédement.
  Cette fonction accepte 1 argument, qui est le nom de la page selectionnée (ex: index.htm)
  Elle récupère le titre de la page dans le tableau "pagesDisponibles".
 */

function affichePageTitre(nompage)
{
    var titreHTML;
    for (i = 0; i < pagesDisponibles.length ; i++)
    {
        if (nompage === pagesDisponibles[i][1])
        {
            titreHTML =  '<div id="titre"><h1>' + pagesDisponibles[i][2] + '</h1></div>'; 
        }
    }
    document.write(titreHTML);
};

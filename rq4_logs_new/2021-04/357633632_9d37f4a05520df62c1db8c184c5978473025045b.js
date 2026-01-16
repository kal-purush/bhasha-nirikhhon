import data from '../data.json';

console.log(data);

const toto = data.media.map(media => media.tags.map(tags => tags, typeof tags));

function onlyUnique(value, index, self) {
    return self.indexOf(value) === index;
  }
  
  // usage example:
  var unique = toto.filter(onlyUnique);
  
  console.log(unique); // ['a', 1, 2, '1']

const bloc_main = document.getElementById("bloc_main");

for (var i in data.photographers) {
    //création de tous les élements nécessaire pour chaque photographe
    const bloc = document.createElement("div");
    const bloc_photographers = document.createElement("a");
    const image = document.createElement("img");
    const photographers = document.createElement("h2");
    const photographers_city = document.createElement("h3");
    const photographers_tagLine = document.createElement("p");
    const photographers_price = document.createElement("p");
    const bloc_photographers_tags = document.createElement("div");

    //affichage de tous les élements nécessaire pour chaque photographe
    photographers.textContent = data.photographers[i].name;
    photographers_city.textContent = data.photographers[i].city + ", " + data.photographers[i].country;
    photographers_tagLine.textContent = data.photographers[i].tagline;
    photographers_price.textContent = data.photographers[i].price + "€/jour";
    image.src =  "../img/Sample_Photos-2/Photographers_ID_Photos/"+ data.photographers[i].portrait;

    //ajout de tous les élements dans la div parent (bloc) qui elle même est ajouté à (bloc_main)
    bloc_main.append(bloc);
    bloc.append(bloc_photographers);
    bloc_photographers.append(image);
    bloc_photographers.append(photographers);
    bloc.append(photographers_city);
    bloc.append(photographers_tagLine);
    bloc.append(photographers_price);
    bloc.append(bloc_photographers_tags);

    //ajout de class à tous les éléments
    bloc.setAttribute("class", "bloc_photographer");
    bloc_photographers.setAttribute("href", "photographe.html?id=" + data.photographers[i].id);
    bloc_photographers.setAttribute("class", "lien_photographe");
    image.setAttribute("class", "img");
    photographers.setAttribute("class", "photographers");
    photographers_city.setAttribute("class", "photographers_city");
    photographers_tagLine.setAttribute("class", "photographers_tagLine");
    photographers_price.setAttribute("class", "photographers_price");
    bloc_photographers_tags.setAttribute("class", "bloc_photographers_tags");

    //ajout de aria-label à tous les éléments
    bloc_photographers.setAttribute("aria-label", data.photographers[i].name);
    photographers_city.setAttribute("aria-label", data.photographers[i].city);
    photographers_tagLine.setAttribute("aria-label", data.photographers[i].tagline);
    photographers_price.setAttribute("aria-label", data.photographers[i].price + "€/jour");

    //boucle sur tous les tags de chaque photographe
    for (var a in data.photographers[i].tags) {

        //création de tous les élements pour les tags de chaque photographe
        const lien_photographers_tags = document.createElement("a");
        const photographers_tags = document.createElement("p");

        //affichage de tous les tags nécessaire pour chaque photographe
        photographers_tags.textContent = "#"+data.photographers[i].tags[a];

        //ajout de tous les tags dans la div parent (bloc_photographers_tags)
        bloc_photographers_tags.append(lien_photographers_tags);
        lien_photographers_tags.append(photographers_tags);

        //ajout de class à tous les éléments
        lien_photographers_tags.setAttribute("class", "lien_tag_photographe");
        photographers_tags.setAttribute("class", "photographers_tags");
        lien_photographers_tags.setAttribute("href", "#");
        photographers_tags.setAttribute("aria-label", data.photographers[i].tags[a]);
    }
}

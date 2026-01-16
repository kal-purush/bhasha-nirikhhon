function word(name, translation, language, region, difficulty) {
    this.name = name;
    this.translation = translation;
    this.language = language;
    this.region = region;
    this.difficulty = difficulty;

    this.changeRegion = function(newRegion){
        this.region = newRegion
    }
    this.addFavorite = function(user){
        user.favorites.add(this.word);
    }
    
}

var backstube = new word("Backstube", "Bakery", "German", "Southern Germany", 2);

wordInfo(
    backstube.name,
    backstube.translation,
    backstube.language,
    backstube.region,
    backstube.difficulty,
)

var krass = new word("Krass", "Awesome", "German", "Germany", 3)

wordInfo(
    krass.name,
    krass.translation,
    krass.language,
    krass.region,
    krass.difficulty,
)
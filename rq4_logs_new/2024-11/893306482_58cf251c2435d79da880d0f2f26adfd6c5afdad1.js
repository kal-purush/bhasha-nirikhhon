const facts = [
    "Bananas are berries, but strawberries aren't!",
    "Honey never spoils. Archaeologists have found pots of honey in ancient tombs.",
    "Octopuses have three hearts and blue blood.",
    "A group of flamingos is called a 'flamboyance.'",
    "Butterflies can taste with their feet.",
    "Cows have best friends and get stressed when separated.",
    "Penguins propose to their mates with a pebble.",
    "Some cats are allergic to humans.",
    "You can't hum while holding your nose closed.",
    "A day on Mercury lasts 1,408 hours.",
    "The fingerprints of a koala are almost identical to humans.",
    "Tigers have striped skin, not just striped fur."
];

function generateFact() {
    const factElement = document.getElementById('fact');
    const randomIndex = Math.floor(Math.random() * facts.length);
    factElement.textContent = facts[randomIndex];
}
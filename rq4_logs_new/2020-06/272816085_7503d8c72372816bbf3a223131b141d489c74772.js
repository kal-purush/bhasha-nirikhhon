import consumer from "./consumer";

const initPokemonCable = () => {
  const pokemonContainer = document.getElementById('pokemon');
  if (pokemonContainer) {
    consumer.subscriptions.create({ channel: "PokemonChannel" }, {
      received(data) {
        console.log(data); // called when data is broadcast in the cable
      },
    });
  }
}

export { initPokemonCable };

// afterbegin
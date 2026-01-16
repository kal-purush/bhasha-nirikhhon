export const evolutions: Function = (chain: any) => {
  let evolutions = [];

  evolutions.push(chain.species.name);

  if (chain.evolves_to.length) {
    evolutions.push(chain.evolves_to[0].species.name);
    if (chain.evolves_to[0].evolves_to[0])
      evolutions.push(chain.evolves_to[0].evolves_to[0].species.name);
  }

  return evolutions;
};
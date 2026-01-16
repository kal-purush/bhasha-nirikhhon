import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { addPokemon } from './Apis';
import { useNavigate } from 'react-router-dom';

const AddPokemon = () => {

  const [pokemonName, setPokemonName] = useState('');
  const [pokemonAbility, setPokemonAbility] = useState('');
  const [pokemonOwnerName, setPokemonOwnerName] = useState('');
  const [pokemonNames, setPokemonNames] = useState([]);
  const [pokemonAbilities, setPokemonAbilities] = useState([]); 
  const [initialPositionX, setInitialPositionX] = useState('');
  const [initialPositionY, setInitialPositionY] = useState('');
  const [speed, setSpeed] = useState('');
  const [direction, setDirection] = useState('');


  const navigate = useNavigate();
  useEffect(() => {
    const fetchPokemonNames = async () => {
      const response = await axios.get('https://pokeapi.co/api/v2/pokemon?limit=100');
      setPokemonNames(response.data.results);
    };

    fetchPokemonNames();
  }, []);

  const handlePokemonNameChange = async (e) => {
    setPokemonName(e.target.value);
    const response = await axios.get(`https://pokeapi.co/api/v2/pokemon/${e.target.value}`);
    const abilities = response.data.abilities.map(ability => ability.ability.name);
    setPokemonAbilities(abilities);
    if (abilities.length === 1) {
      setPokemonAbility(abilities[0]);
    } else {
      setPokemonAbility('');
    }
  };

  const handleSubmit = async (e) => {
    console.log("handel submit called");
    e.preventDefault();
    const trainerData = {
      "pokemonOwnerName": pokemonOwnerName,
      "pokemons": {
        "pokemonName" : pokemonName,
        "pokemonAbility":pokemonAbilities[0],
        "initialPositionX": parseInt(initialPositionX, 10),
        "initialPositionY": parseInt(initialPositionY, 10),
        "speed": parseInt(speed, 10),
        "direction":direction
      }
    };
    try {
        const response = await addPokemon(trainerData);
        console.log(response);
        alert("PokeMon added successfully in the  database !")
        navigate('/')
      } catch (error) {
        console.log(error);
      }
    };




  return (
    <form onSubmit={handleSubmit}>
      <input
        type="text"
        placeholder="Pokemon Owner Name"
        value={pokemonOwnerName}
        onChange={(e) => setPokemonOwnerName(e.target.value)}
        required
      />
      <select className="selecttt" value={pokemonName} onChange={handlePokemonNameChange} required>
        <option className="selectt"value="">Select Pokemon</option>
        {pokemonNames.map((pokemon) => (
          <option key={pokemon.name} value={pokemon.name}>{pokemon.name}</option>
        ))}
      </select>
      <select className="selecttt" value={pokemonAbility} onChange={(e) => setPokemonAbility(e.target.value)} required>
        <option className="selectt" value="">Select Ability</option>
        {pokemonAbilities.map((ability) => (
          <option key={ability} value={ability}>{ability}</option>
        ))}
      </select>
      <input
        type="text"
        placeholder="Initial Position X"
        value={initialPositionX}
        onChange={(e) => setInitialPositionX(e.target.value)}
        required
      />
      <input
        type="text"
        placeholder="Initial Position Y"
        value={initialPositionY}
        onChange={(e) => setInitialPositionY(e.target.value)}
        required
      />
      <input
        type="text"
        placeholder="Speed"
        value={speed}
        onChange={(e) => setSpeed(e.target.value)}
        required
      />
      <input
        type="text"
        placeholder="Direction"
        value={direction}
        onChange={(e) => setDirection(e.target.value)}
        required
      />
      <button className = " button-style"type="submit">Add User</button>
    </form>
  );
};

export default AddPokemon;
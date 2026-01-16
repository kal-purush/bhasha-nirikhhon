import React from 'react';
import { useParams } from 'react-router-dom';
import useFetch from '../Hooks/useFetch';
import Loading from '../Components/Helpers/Loading';
import { useNavigate } from 'react-router';
import { FaArrowRight, FaArrowLeft } from 'react-icons/fa';

import './Details.css';

const Details = () => {
    const navigate = useNavigate();
    const params = useParams();
    const { request, loading, error } = useFetch();
    const [pokemon, setPokemon] = React.useState();
    const [currentSprite, setCurrentSprite] = React.useState(0);

    React.useEffect(() => {
        const { name } = params;

        function filterPokemonInfo (pokemon) {
            const name = pokemon.name.charAt(0).toUpperCase() + pokemon.name.slice(1);
            const types = pokemon.types.map(type => type.type.name);
            const abilities = pokemon.abilities.map(ability => ability.ability.name);
            const height = (pokemon.height * 10).toFixed();
            const weight = (pokemon.weight / 10).toFixed(1);
            
            let sprites = [];
            for (let spriteName in pokemon.sprites) {
                const necessarySprites = ['front_default', 'back_default'];

                if (necessarySprites.includes(spriteName)) {
                    sprites.push(pokemon.sprites[spriteName])
                }
            }
            sprites.reverse();

            const stats = pokemon.stats.map(stat => {
                const name = stat.stat.name.charAt(0).toUpperCase() + stat.stat.name.slice(1);

                return { 
                    name,
                    value: stat.base_stat
                }
            });

            return { name, types, abilities, height, weight, sprites, stats };
        }

        async function fetchPokemonDetails() {
            let pokemonName = name.toLowerCase();
            const { json } = await request(`https://pokeapi.co/api/v2/pokemon/${pokemonName}`);
            
            if (!json) {
                navigate('/notFound');
                return;
            }

            let pokemonDetails = filterPokemonInfo(json);

            setPokemon(pokemonDetails);
        }

        fetchPokemonDetails();
    }, [request, params, navigate]);

    function showNextSprite() {
        const nextIndex = currentSprite + 1;

        if (pokemon.sprites[nextIndex]) {
            setCurrentSprite(nextIndex);
        }
        else {
            setCurrentSprite(0);
        }
    }

    function showPreviousSprite() {
        const previousIndex = currentSprite - 1;

        if (pokemon.sprites[previousIndex]) {
            setCurrentSprite(previousIndex);
        }
        else {
            setCurrentSprite(3);
        }
    }

    if (pokemon) {
        return (
            <main className="details-container">
                <label>{pokemon.name}</label>

                <section className="image-slide">
                    <div className="arrow-icon">
                        <FaArrowLeft onClick={showPreviousSprite} />
                    </div>

                    <img
                        key={currentSprite}
                        src={pokemon.sprites[currentSprite]}
                        alt="Pokemon sprite"
                    />

                    <div className="arrow-icon">
                        <FaArrowRight onClick={showNextSprite} />
                    </div>
                </section>

                <section className="info-table">
                    <h3 className="row"> Basic </h3>

                    <div className="row">
                        <span className="title"> Type </span>
                        <div className="divider" />
                        <span className="info">
                            <div className="types">
                                {pokemon.types.map(type => (
                                    <img
                                        key={type + pokemon.name}
                                        src={`https://veekun.com/dex/media/types/en/${type}.png`}
                                        alt={`${type} type`}
                                    />
                                ))}
                            </div>
                        </span>
                    </div>

                    <div className="row">
                        <span className="title"> Height </span>

                        <div className="divider" />

                        <span className="info">{pokemon.height} cm</span>
                    </div>

                    <div className="row">
                        <span className="title"> Weight </span>

                        <div className="divider" />

                        <span className="info">{pokemon.weight} kg</span>
                    </div>
                </section>

                <section className="info-table">
                    <h3 className="row"> Stats </h3>

                    {pokemon.stats.map(stat => (
                        <div key={stat.name} className="row">
                            <span className="title">{stat.name}</span>

                            <div className="divider" />

                            <span className="info">{stat.value}</span>
                        </div>
                    ))}
                </section>

                <section className="info-table">
                    <h3 className="row"> Abilities </h3>

                    {pokemon.abilities.map((ability, index) => (
                        <div key={ability} className="row">
                            <span className="title">{`Slot ${index+1}`}</span>

                            <div className="divider" />

                            <span className="info">{ability}</span>
                        </div>
                    ))}
                </section>
            </main>
        );
    }
    else if (error) {
        return (
            <p>{error}</p>
        );
    }
    else if (loading) {
        return (
            <Loading />
        );
    }
    else {
        return null;
    }
}

export default Details;
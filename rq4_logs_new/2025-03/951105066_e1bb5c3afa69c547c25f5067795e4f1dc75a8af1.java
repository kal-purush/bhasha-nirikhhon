package com.exampleaa.SimpleCRUD.Mapper;
import com.exampleaa.SimpleCRUD.DTO.PokemonDTO;
import com.exampleaa.SimpleCRUD.Entity.Pokemon;
import org.springframework.stereotype.Component;

@Component
public class PokemonMapper {
    public PokemonDTO toDTO(Pokemon pokemon) {
        return new PokemonDTO(
                pokemon.getId(),
                pokemon.getName(),
                pokemon.getType(),
                pokemon.getLevel(),
                pokemon.getGender(),
                pokemon.getHeight(),
                pokemon.getWeight(),
                pokemon.isLegendary()
        );
    }

    public Pokemon toEntity(PokemonDTO pokemonDTO) {
        Pokemon pokemon = new Pokemon();
        pokemon.setName(pokemonDTO.getName());
        pokemon.setType(pokemonDTO.getType());
        pokemon.setLevel(pokemonDTO.getLevel());
        pokemon.setGender(pokemonDTO.getGender());
        pokemon.setHeight(pokemonDTO.getHeight());
        pokemon.setWeight(pokemonDTO.getWeight());
        pokemon.setLegendary(pokemonDTO.isLegendary());
        return pokemon;
    }
}

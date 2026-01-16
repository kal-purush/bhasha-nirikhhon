using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using RestPokemon.PokemonData;
using RestPokemon.Models;

namespace RestPokemon.Controllers
{
    [ApiController]
    public class PokemonsController : ControllerBase
    {
        private IPokemonData _pokemonData;

        public PokemonsController(IPokemonData pokemonData)
        {
            _pokemonData = pokemonData;
        }

        [HttpGet]
        [Route("api/[controller]")]
        public IActionResult GetPokemons()
        {
            return Ok(_pokemonData.GetPokemons());
        }

        [HttpGet]
        [Route("api/[controller]/{id}")]
        public IActionResult GetPokemon(int id)
        {
            var pokemon = _pokemonData.GetPokemon(id);

            if (pokemon != null)
            {
                return Ok(pokemon);
            }
            return NotFound($"Pokemon with Id: {id} was not found");
        }

        [HttpPost]
        [Route("api/[controller]")]
        public IActionResult GetPokemon(Pokemon pokemon)
        {
            _pokemonData.AddPokemon(pokemon);
            return Created(HttpContext.Request.Scheme + "://" + HttpContext.Request.Host + HttpContext.Request.Path + "/" + pokemon.Id, pokemon);
        }

        [HttpDelete]
        [Route("api/[controller]/{id}")]
        public IActionResult DeletePokemon(int id)
        {
            var pokemon = _pokemonData.GetPokemon(id);

            if (pokemon != null)
            {
                _pokemonData.DeletePokemon(pokemon);
                return Ok();
            }
            return NotFound($"Pokemon with Id: {id} was not found");
        }

        [HttpPatch]
        [Route("api/[controller]/{id}")]
        public IActionResult EditPokemon(int id, Pokemon pokemon)
        {
            var exitsPokemon = _pokemonData.GetPokemon(id);

            if (exitsPokemon != null)
            {
                pokemon.Id = exitsPokemon.Id;
                _pokemonData.EditPokemon(pokemon);
            }
            return Ok(pokemon);
        }
    }
}
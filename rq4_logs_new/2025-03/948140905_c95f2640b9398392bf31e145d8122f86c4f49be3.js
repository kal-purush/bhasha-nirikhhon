import React from "react";
import { useParams } from "react-router-dom";
import { Card, CardContent, CardMedia, Typography, List, ListItem, Divider, Box } from "@mui/material";
import PokemonEvolution from "./PokemonEvolution";

const PokemonDetail = ({ data }) => {
  const { id } = useParams();
  const pokemon = data.find((p) => p.id === parseInt(id));

  if (!pokemon) {
    return <Typography variant="h5">Pokémon no encontrado</Typography>;
  }

  return (
    <Box sx={{ display: "flex", flexDirection: "column", alignItems: "center", padding: 2 }}>
      {/* Contenedor de imagen y detalles en dos columnas */}
      <Card sx={{ display: "flex", flexDirection: { xs: "column", md: "row" }, maxWidth: 700, padding: 2 }}>
        
        {/* Imagen del Pokémon */}
        <CardMedia
          component="img"
          image={pokemon.sprites.front_default}
          alt={pokemon.name}
          sx={{ width: 200, height: 200, margin: "auto" }}
        />

        {/* Detalles del Pokémon */}
        <CardContent sx={{ flex: 1 }}>
          <Typography variant="h4" gutterBottom>{pokemon.name.toUpperCase()}</Typography>
          <Typography variant="subtitle1" sx={{ fontStyle: "italic" }}>{pokemon.description}</Typography>
          <Typography variant="body1">Altura: {pokemon.height / 10} m</Typography>
          <Typography variant="body1">Peso: {pokemon.weight / 10} kg</Typography>
          <Typography variant="body1">Experiencia Base: {pokemon.base_experience}</Typography>
          
          <Typography variant="h6" sx={{ marginTop: 2 }}>Tipos:</Typography>
          <Typography variant="body1">{pokemon.types.map((t) => t.type.name).join(", ")}</Typography>

          <Typography variant="h6" sx={{ marginTop: 2 }}>Habilidades:</Typography>
          <List>
            {pokemon.abilities.map((a) => (
              <ListItem key={a.ability.name}>{a.ability.name}</ListItem>
            ))}
          </List>

          <Typography variant="h6" sx={{ marginTop: 2 }}>Estadísticas:</Typography>
          <List>
            {pokemon.stats.map((s) => (
              <ListItem key={s.stat.name}>{s.stat.name.toUpperCase()}: {s.base_stat}</ListItem>
            ))}
          </List>
        </CardContent>
      </Card>

      {/* Evoluciones del Pokémon */}
      <Divider sx={{ width: "80%", marginY: 3 }} />
      <Typography variant="h5" sx={{ marginBottom: 2 }}>Evoluciones</Typography>

      <Box sx={{ display: "flex", gap: 2, flexWrap: "wrap", justifyContent: "center" }}>
        {pokemon.evolutions.map((evo) => (
          <PokemonEvolution key={evo.id} pokemon={evo} />
        ))}
      </Box>
    </Box>
  );
};

export default PokemonDetail;
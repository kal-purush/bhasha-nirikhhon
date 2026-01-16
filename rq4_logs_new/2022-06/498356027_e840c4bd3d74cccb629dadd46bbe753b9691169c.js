import { useHttpClient } from '../../shared-hooks/http-hook'
import { editPokemon } from '../../redux/main/post_updatePokemon';
import { useDispatch } from 'react-redux';


const IndividualPokemon = ({ pokemon }) => {

    const { deletePokemon } = useHttpClient()
    const dispatch = useDispatch();



    return (
        <tr key={pokemon.id}>
          <td>{pokemon.name}</td>
          <td className="pokemon_image">
            <img src={pokemon.image} width="25" alt="pokemon_image" />
          </td>
          <td>{pokemon.attack}</td>
          <td>{pokemon.defense}</td>
          <td>
            <div className="pokemon_action_buttons">
              <div
                className="pokemon_edit_icon"
                onClick={() =>
                  dispatch(
                    editPokemon(
                      pokemon.id,
                      pokemon.name,
                      pokemon.image,
                      pokemon.attack,
                      pokemon.defense
                    )
                  )
                }
              >
                <ion-icon name="create-outline"></ion-icon>
              </div>
      
              <div className="pokemon_delete_icon">
                <ion-icon
                  name="trash-outline"
                  onClick={() => deletePokemon(pokemon.id)}
                ></ion-icon>
              </div>
            </div>
          </td>
        </tr>
      )
};

export default IndividualPokemon;
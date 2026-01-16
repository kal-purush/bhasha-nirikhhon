import SearchIcon from "@material-ui/icons/Search";
import CloseIcon from "@material-ui/icons/Close";
import useSearch from "../../hooks/useSearch";
import * as S from './SearchBar.style';
import useCart from "../../hooks/useCart";


function SearchBar() {

  const { handleFilter, clearInput, filteredData, wordEntered, handleKeyDown, index } = useSearch();
  const { imgUrl, addToCartList } = useCart();

  return (
    <div>
      <S.searchInputs>
        <S.Input
          type="text"
          placeholder="Busca..."
          value={wordEntered}
          onChange={handleFilter}
          onKeyDown={handleKeyDown}
        />
        <S.IconSearch>
          {filteredData.length === 0 ? (
            <SearchIcon />
          ) : (
            <CloseIcon id="clearBtn" onClick={clearInput} onKeyDown={handleKeyDown} />
          )}
        </S.IconSearch>
      </S.searchInputs>
      {filteredData.length !== 0 && (
        <S.resultado onKeyDown={handleKeyDown}>
          {filteredData.map((pokemon, key) => {
            return (

              <S.name key={key}>
                <S.PokemonName>{pokemon.name}</S.PokemonName>
                <S.Button onClick={() => addToCartList(index(`${pokemon.name}`), `${pokemon.name}`, 1000, index(`${pokemon.name}`))}>Comprar</S.Button>
                <S.Img src={imgUrl(index(`${pokemon.name}`))} />
              </S.name>
            );
          })}
        </S.resultado>
      )}
    </div>
  );
}

export default SearchBar;
import { Link } from "react-router-dom";

const Menu = ({ menu, token, handleToken, setMenu }) => {
  return (
    <div className={menu ? "menu" : "hide-menu"}>
      {token ? (
        <button
          style={{ backgroundColor: "red" }}
          className="deconnexion"
          onClick={() => {
            handleToken(null);
            setMenu(!menu);
          }}
        >
          Se deconnecter
        </button>
      ) : (
        <div className="login-signup">
          <Link to="/signup">
            <button
              onClick={() => {
                setMenu(!menu);
              }}
            >
              S'inscrire
            </button>
          </Link>
          <Link to="/login">
            <button
              onClick={() => {
                setMenu(!menu);
              }}
            >
              Se connecter
            </button>
          </Link>
        </div>
      )}
      <Link to="/publish">
        <button
          className="publish"
          onClick={() => {
            setMenu(!menu);
          }}
        >
          Vends tes articles
        </button>
      </Link>
    </div>
  );
};

export default Menu;
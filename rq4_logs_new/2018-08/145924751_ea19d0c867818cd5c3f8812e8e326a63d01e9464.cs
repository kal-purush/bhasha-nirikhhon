using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerController : MonoBehaviour {

    #region Player Class

    /// <summary>
    /// Player class holds the player's information that belongs to them
    /// </summary>
    public class Player {
        // Set class attributes
        #region Class Attributes

        private int _id;
        private string _name;
        private string _color;
        //private List<Stock> _stocks;
        private List<Tile> _tiles;


        public int Id {
            get { return _id; }
            set { _id = value; }
        }

        public string Name {
            get { return _name; }
            set { _name = value; }
        }

        public string Color {
            get { return _color; }
            set { _color = value; }
        }

        //public List<Stocks> Stocks {
        //    get { return _stocks; }
        //    set { _stocks = value; }
        //}

        public List<Tile> Tiles {
            get { return _tiles; }
            set { _tiles = value; }
        }

        public Player(int id, string name, string color, List<Tile> tiles) {
            _id = id;
            _name = name;
            _color = color;
            //_stocks = stocks;
            _tiles = tiles;
        }

        #endregion
    }

    #endregion

    #region Class Imports

    public List<Tile> tiles;

    #endregion

    #region Class Attributes
    
    private Player[] players;

    enum Colors { red, green, blue, cyan, magenta, yellow };

    #endregion

    #region Unity Specific

    // Use this for initialization
    void Start() {
        //CreatePlayers(3);
    }

    // Update is called once per frame
    void Update() {

    }

    #endregion

    #region Class Functions

    public void CreatePlayers(int numberOfPlayers = 3) {
        players = new Player[numberOfPlayers];
        for (int i = 0; i < numberOfPlayers; ++i)
            players[i] = new Player(i, "Player 1", GetColorById(i));
    }

    /// <summary>
    /// Supply an enum int-value and get the string-value
    /// </summary>
    /// <param name="id">int value</param>
    /// <returns>string value</returns>
    public string GetColorById(int id) {
        Colors color = (Colors)id;
        return color.ToString();
    }

    public string GetPlayerName(int id) {
        return players[id].Name;
    }

    public string SetPlayerName(int id, string newName) {
        players[id].Name = newName;
        return players[id].Name;
    }

    public void GetPlayerStocks(int id) {
        // TODO: return stocks
    }

    public void GetPlayerTiles(int id) {
        // TODO: return tiles
    }

    #endregion
}
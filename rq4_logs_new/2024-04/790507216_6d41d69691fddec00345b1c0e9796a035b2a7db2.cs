using System;
using System.Collections.Generic;
using System.Linq;
using UnityEngine;
using Random = System.Random;

namespace IronMountain.SlidePuzzles
{
    public class SlidePuzzle : MonoBehaviour
    {
        public event Action OnSolvedChanged;
    
        [Header("Settings")]
        [SerializeField] private bool spawnOnStart;
        [SerializeField] private bool shuffleOnStart;
        [SerializeField] private int size;
        [SerializeField] private Texture2D texture;
        
        [Header("References")]
        [SerializeField] private SlidePuzzlePiece piecePrefab;
        [SerializeField] private Transform pieceParent;
        
        [Header("Cache")]
        private List<SlidePuzzlePiece> _pieces = new ();
        private Vector2Int _emptySpace = Vector2Int.zero;
        private bool _solved;
        private bool _shuffling;

        private readonly Dictionary<Vector2Int, SlidePuzzlePiece> _piecesDictionary = new ();

        public int Size => size;
        
        public Vector2Int EmptySpace
        {
            get => _emptySpace;
            set => _emptySpace = value;
        }

        public bool Solved
        {
            get => _solved;
            private set
            {
                if (_solved == value) return;
                _solved = value;
                OnSolvedChanged?.Invoke();
            }
        }
        
        private void OnValidate()
        {
            if (!spawnOnStart) shuffleOnStart = false;
            size = Mathf.Clamp(size, 2, 10);
        }

        public void Initialize(int size, Texture2D texture, bool shuffle = true)
        {
            this.size = size;
            this.texture = texture;
            SpawnPieces();
            if (shuffle) Shuffle();
        }

        public void SetPiece(Vector2Int coordinates, SlidePuzzlePiece piece)
        {
            if (!_piecesDictionary.ContainsKey(coordinates)) _piecesDictionary.Add(coordinates, piece);
            else _piecesDictionary[coordinates] = piece;
        }

        public SlidePuzzlePiece GetPiece(Vector2Int coordinates)
        {
            return _piecesDictionary.ContainsKey(coordinates)
                ? _piecesDictionary[coordinates] 
                : null;
        }

        private void Start()
        {
            if (spawnOnStart) SpawnPieces();
            if (shuffleOnStart) Shuffle();
        }

        private void DestroyPieces()
        {
            foreach (SlidePuzzlePiece piece in _pieces)
            {
                Destroy(piece.gameObject);
            }
            _pieces.Clear();
            _piecesDictionary.Clear();
        }

        private void SpawnPieces()
        {
            DestroyPieces();
            if (!texture) return;
            int textureSegmentWidth = texture ? Mathf.FloorToInt((float) texture.width / size) : 0;
            int textureSegmentHeight = texture ? Mathf.FloorToInt((float) texture.height / size) : 0;
            for (int y = 0; y < size; y++)
            {
                for (int x = 0; x < size; x++)
                {
                    if (y == size - 1 && x == size - 1) continue;
                    Vector2Int trueCoordinates = new Vector2Int(x, y);
                    SlidePuzzlePiece piece = Instantiate(piecePrefab, pieceParent).Initialize(this, trueCoordinates);
                    piece.name = "Slide Puzzle Piece (" + x + ", " + y + ")";
                    SetPiece(trueCoordinates, piece);
                    piece.OnCurrentCoordinatesChanged += OnPieceCoordinatesChanged;
                    if (texture)
                    {
                        Texture2D segment = new Texture2D(textureSegmentWidth, textureSegmentHeight);
                        segment.SetPixels(texture.GetPixels(
                            textureSegmentWidth * x,
                            textureSegmentHeight * (size - 1 - y),
                            textureSegmentWidth,
                            textureSegmentHeight
                        ));
                        segment.Apply();
                        piece.SetSprite(Sprite.Create(segment, new Rect(0,0,textureSegmentWidth, textureSegmentHeight), Vector2.one / 2));
                    }
                    _pieces.Add(piece);
                }
            }
            _emptySpace = new Vector2Int(size - 1, size - 1);
            SetPiece(_emptySpace, null);
        }

        private void OnPieceCoordinatesChanged()
        {
            if (_shuffling) return;
            _pieces.Sort((pieceA, pieceB) => pieceA.CurrentValue.CompareTo(pieceB.CurrentValue));
            Solved = TestSolution();
        }

        private bool TestSolution() => _pieces.TrueForAll(piece => piece.CurrentCoordinates == piece.TrueCoordinates);

        private int CountInversions()
        {
            int inversions = 0;
            for (int i = 0; i < _pieces.Count - 1; i++)
            {
                for (int j = i + 1; j < _pieces.Count; j++)
                {
                    if (!_pieces[j] || !_pieces[i]) continue;
                    if (_pieces[j].TrueValue < _pieces[i].TrueValue)
                    {
                        inversions++;
                    } 
                }
            }
            return inversions;
        }
        
        private bool ShuffleIsValid()
        {
            return size % 2 == 0
                ? (CountInversions() + size - 1) % 2 == 1
                : CountInversions() % 2 == 0;
        }

        public void Shuffle()
        {
            if (_pieces.Count != size * size - 1) return;
            _shuffling = true;
            Random randomNumberGenerator = new Random();
            do
            {
                _pieces = _pieces.OrderBy(piece => randomNumberGenerator.Next()).ToList();
                int index = 0;
                for (int y = 0; y < size; y++)
                {
                    for (int x = 0; x < size; x++)
                    {
                        if (y == size - 1 && x == size - 1) continue;
                        Vector2Int coordinates = new Vector2Int(x, y);
                        _pieces[index].SetCurrentCoordinates(coordinates);
                        SetPiece(coordinates, _pieces[index]);
                        index++;
                    }
                }
            }
            while (!ShuffleIsValid() || TestSolution());
            Solved = false;
            _emptySpace = new Vector2Int(size - 1, size - 1);
            SetPiece(_emptySpace, null);
            _shuffling = false;
        }

        public bool IsValidPieceToMove(Vector2Int coordinates)
        {
            return _emptySpace.x == coordinates.x && _emptySpace.y == coordinates.y + 1
                   || _emptySpace.x == coordinates.x && _emptySpace.y == coordinates.y - 1
                   || _emptySpace.x == coordinates.x + 1 && _emptySpace.y == coordinates.y
                   || _emptySpace.x == coordinates.x - 1 && _emptySpace.y == coordinates.y;
        }
        
        public bool IsValidLineToSlide(Vector2Int coordinates)
        {
            return _emptySpace.x == coordinates.x || _emptySpace.y == coordinates.y;
        }
    }
}
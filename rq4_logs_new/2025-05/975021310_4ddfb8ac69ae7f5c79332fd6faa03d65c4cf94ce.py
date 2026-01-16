import pytest
from flask import Flask
from app import app, check_winner, is_board_full  # Assuming your Flask app is in app.py

@pytest.fixture
def client():
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

def test_check_winner_row(client):
    board = ['X', 'X', 'X', ' ', ' ', ' ', ' ', ' ', ' ']
    assert check_winner(board) == 'X'

def test_check_winner_col(client):
    board = ['X', ' ', ' ', 'X', ' ', ' ', 'X', ' ', ' ']
    assert check_winner(board) == 'X'

def test_check_winner_diag(client):
    board = ['X', ' ', ' ', ' ', 'X', ' ', ' ', ' ', 'X']
    assert check_winner(board) == 'X'

def test_check_winner_no_winner(client):
    board = ['X', 'O', ' ', ' ', ' ', ' ', ' ', ' ', ' ']
    assert check_winner(board) is None

def test_is_board_full_full(client):
    board = ['X', 'O', 'X', 'O', 'X', 'O', 'X', 'O', 'X']
    assert is_board_full(board) is True

def test_is_board_full_not_full(client):
    board = ['X', 'O', 'X', 'O', 'X', 'O', 'X', 'O', ' ']
    assert is_board_full(board) is False

def test_play_route_get(client):
    response = client.get('/play')
    assert response.status_code == 200
    assert b'Tic-Tac-Toe' in response.data

def test_play_route_post_valid_move(client):
    response = client.post('/play', data={'position': '0'}, query_string={'board': [' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' '], 'player': 'X', 'game_over': 'false'})
    assert response.status_code == 302  # Redirect
    # Follow the redirect to check the new board state (optional)
    response = client.get(response.location)
    assert b'X' in response.data

def test_play_route_post_invalid_move(client):
     response = client.post('/play', data={'position': '0'}, query_string={'board': ['X', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' '], 'player': 'O', 'game_over': 'false'})
     response = client.get(response.location)
     assert b'X' in response.data

def test_play_route_post_game_over(client):
    response = client.post('/play', data={'position': '0'}, query_string={'board': ['X', 'O', 'X', 'O', 'X', 'O', 'X', 'O', 'X'], 'player': 'X', 'game_over': 'true'})
    assert response.status_code == 200
    assert b'Draw' in response.data or b'wins' in response.data

def test_reset_route(client):
    response = client.get('/reset')
    assert response.status_code == 302
    assert response.location == '/play'
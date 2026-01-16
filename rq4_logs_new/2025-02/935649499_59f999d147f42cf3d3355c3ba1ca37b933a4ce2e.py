import copy
from colorama import Fore, Style, init

# Dimensions du plateau
BOARD_SIZE = 7

# Directions possibles pour les mouvements
DIRECTIONS = [(0, 1), (1, 0), (0, -1), (-1, 0)]

init()
def print_board(board):
    """Affiche le plateau avec des contours en bleu."""

    # Affiche la bordure supérieure en bleu
    print(Fore.BLUE + "┌" + "─" * (2 * BOARD_SIZE - 1) + "┐" + Style.RESET_ALL)
    
    for row in board:
        # Affiche les cellules avec des séparateurs verticaux en bleu
        print(Fore.BLUE + "│" + Fore.RESET + " ".join("\u2022" if cell == 1 else " " for cell in row) + Fore.BLUE + "│" + Style.RESET_ALL)
    
    # Affiche la bordure inférieure en bleu
    print(Fore.BLUE + "└" + "─" * (2 * BOARD_SIZE - 1) + "┘" + Style.RESET_ALL)
    print()

# Vérifie si un mouvement est valide
def is_valid_move(board, x, y, dx, dy):
    nx, ny = x + dx, y + dy  # Coordonnées de la case sautée
    nnx, nny = x + 2 * dx, y + 2 * dy  # Coordonnées de la case cible

    # Vérifications des limites et conditions du jeu
    if not (0 <= nx < BOARD_SIZE and 0 <= ny < BOARD_SIZE and
            0 <= nnx < BOARD_SIZE and 0 <= nny < BOARD_SIZE):
        return False

    return (
        board[x][y] == 1  # Case d'origine contient une bille
        and board[nx][ny] == 1  # Case sautée contient une bille
        and board[nnx][nny] == 0  # Case cible est vide
    )


# Effectue un mouvement
def make_move(board, x, y, dx, dy):
    nx, ny = x + dx, y + dy
    nnx, nny = x + 2 * dx, y + 2 * dy
    board[x][y] = 0  # Retirer la bille d'origine
    board[nx][ny] = 0  # Retirer la bille sautée
    board[nnx][nny] = 1  # Placer la bille à la destination


# Annule un mouvement
def undo_move(board, x, y, dx, dy):
    nx, ny = x + dx, y + dy
    nnx, nny = x + 2 * dx, y + 2 * dy
    board[x][y] = 1  # Remettre la bille d'origine
    board[nx][ny] = 1  # Remettre la bille sautée
    board[nnx][nny] = 0  # Vider la case de destination


# Vérifie si une solution est atteinte
def is_solved(board, x, y):
    count = sum(row.count(1) for row in board)
    return count == 1 and board[x][y] == 1


# Résolution avec backtracking
visited_states = set()

def solve(board, moves, target_x, target_y):
    state = tuple(map(tuple, board))  # Convertir le plateau en un état immuable
    if state in visited_states:
        return False  # État déjà visité
    visited_states.add(state)

    # Vérifier si la condition de victoire est remplie
    if is_solved(board, target_x, target_y):
        return True

    # Parcourir toutes les cases du plateau
    for x in range(len(board)):
        for y in range(len(board[0])):
            if board[x][y] == 1:  # Trouver une bille
                for dx, dy in DIRECTIONS:
                    if is_valid_move(board, x, y, dx, dy):
                        make_move(board, x, y, dx, dy)
                        moves.append((x, y, dx, dy))

                        if not solve(board, moves, target_x, target_y):
                            moves.pop()
                            undo_move(board, x, y, dx, dy)
                        else:
                            return True

    return False


def ask_to_change_empty_spot():
    """Demande à l'utilisateur s'il souhaite changer la case vide."""
    while True:
        response = input("Souhaitez-vous changer la case vide ? (o/n) : ").strip().lower()
        if response in ('o', 'n'):
            return response == 'o'
        print("Réponse invalide. Veuillez entrer 'o' pour oui ou 'n' pour non.")

def choose_empty_spot(board):
    """Permet à l'utilisateur de choisir la nouvelle case vide et remplit l'ancienne."""
    # Trouver la case vide actuelle
    current_empty = None
    for i in range(BOARD_SIZE):
        for j in range(BOARD_SIZE):
            if board[i][j] == 0:
                current_empty = (i, j)
                break
        if current_empty:
            break

    while True:
        print("\nPlateau actuel :")
        print_board(board)
        try:
            x = int(input("Entrez la ligne (0-6) de la nouvelle case vide : "))
            y = int(input("Entrez la colonne (0-6) de la nouvelle case vide : "))
            if (x, y) == current_empty:
                print("La case vide reste inchangée.")
                return x, y
            if board[x][y] == 1:  # Vérifier que la case choisie est jouable
                # Remplacer l'ancienne case vide par une bille
                if current_empty:
                    board[current_empty[0]][current_empty[1]] = 1
                # Définir la nouvelle case vide
                board[x][y] = 0
                return x, y
            else:
                print("Position invalide. Choisissez une case jouable (1).")
        except (ValueError, IndexError):
            print("Coordonnées non valides. Essayez encore.")

def choose_final_pos():
    target_x = 3
    target_y = 3
    #print("Souhaitez-vous imposer une position finale pour la dernière bille ? (o/n)")
    if input("Souhaitez-vous imposer une position finale pour la dernière bille ? (o/n) : ").lower() == "o":
        while True:
            try:
                final_x = int(input("Entrez la ligne cible (0-6) : "))
                final_y = int(input("Entrez la colonne cible (0-6) : "))
                if (final_x, final_y) == (target_x, target_y):
                    print("La position finale est identique à la position initiale de la case vide.")
                    break
                else:
                    target_x, target_y = final_x, final_y
                    print(f"Position finale choisie : ({target_x}, {target_y})")
                    break
            except (ValueError, IndexError):
                print("Coordonnées non valides. Essayez encore.")
    else:
        print(f"Position finale non modifiée : ({target_x}, {target_y})")


def main():
    # Plateaux prédéfinis
    CLASSIC_ADJUSTED_BOARD = [
        [-1, -1,  1,  1,  1, -1, -1],
        [-1, -1,  1,  1,  1, -1, -1],
        [ 1,  1,  1,  1,  1,  1,  1],
        [ 1,  1,  1,  0,  1,  1,  1],
        [ 1,  1,  1,  1,  1,  1,  1],
        [-1, -1,  1,  1,  1, -1, -1],
        [-1, -1,  1,  1,  1, -1, -1],
    ]

    CIRCLE_CENTERED_BOARD = [
        [-1, -1,  1,  1,  1, -1, -1],
        [-1,  1,  1,  1,  1,  1, -1],
        [ 1,  1,  1,  1,  1,  1,  1],
        [ 1,  1,  1,  0,  1,  1,  1],
        [ 1,  1,  1,  1,  1,  1,  1],
        [-1,  1,  1,  1,  1,  1, -1],
        [-1, -1,  1,  1,  1, -1, -1],
]

    # Choisir le plateau
    print("Choisissez un plateau :")
    print("1 = Plateau Classique Ajusté")
    print("2 = Plateau Cercle Centré")
    choice = input("Votre choix : ")
    
    target_x = 3
    target_y = 3
    empty_x = 3
    empty_y = 3
    
    if choice == "2":
        board = copy.deepcopy(CIRCLE_CENTERED_BOARD)
    else:
        board = copy.deepcopy(CLASSIC_ADJUSTED_BOARD)

    if ask_to_change_empty_spot():
        empty_x, empty_y = choose_empty_spot(board)
    else:
        print(f"case vide non modifiée : ({empty_x}, {empty_y})")               

    # Permettre de choisir la position finale
    choose_final_pos()
        
    # Résolution
    moves = []
    print("\nPlateau initial :")
    print_board(board)

    if solve(board, moves, target_x, target_y):
        print("\nSolution trouvée avec les mouvements suivants :")
        for move in moves:
            x, y, dx, dy = move
            print(f"Bille de ({x}, {y}) sautée en direction ({x + 2 * dx}, {y + 2 * dy})")
    else:
        print("\nAucune solution trouvée.")

    print("\nPlateau final :")
    print_board(board)


if __name__ == "__main__":
    main()
def on_click(event, selected_piece, current_player, pieces, board, draw_board, images):
    row = event.y // 50
    col = event.x // 50
    # Dra        w a rectangle around the selected square
    x0 = col * 50
    y0 = row * 50
    x1 = x0 + 50
    y1 = y0 + 50
    board.create_rectangle(x0, y0, x1, y1, outline="red", width=3, tags="selected_square")
    if selected_piece:
        # Check if the move is valid
        if selected_piece.is_valid_move(row, col, pieces):
            # Move the selected piece to the clicked square
            selected_piece.row = row
            selected_piece.col = col
            # Check if a piece of the opposite color was captured
            for piece in pieces:
                if piece.row == row and piece.col == col and piece.color != selected_piece.color:
                    pieces.remove(piece)
                    break
            draw_board(board, pieces, images)
            # Delete the rectangle around the selected square
            board.delete("selected_square")
            # Switch to the other player
            if current_player == "White":
                current_player = "Black"
            else:
                current_player = "White"
        selected_piece = None      
        board.delete("selected_square")

    else:
        # Check if a piece of the current player's color was clicked
        for piece in pieces:
            if piece.row == row and piece.col == col and piece.color == current_player:
                selected_piece = piece
                break
    return selected_piece, current_player

            
def draw_board(board, pieces, images):
    board.delete("all")
    for i in range(8):
        for j in range(8):
            if (i + j) % 2 == 0:
                color = "white"
            else:
                color = "gray"
            board.create_rectangle(i * 50, j * 50, (i + 1) * 50, (j + 1) * 50, fill=color)
    for piece in pieces:
        if piece.color == "White":
            color = "white"
        else:
            color = "black"
        image_name = f"pieces/{color}_{piece.piece_type.lower()}.png"
        image = images[image_name]
        board.create_image(piece.col * 50 + 25, piece.row * 50 + 25, image=image)
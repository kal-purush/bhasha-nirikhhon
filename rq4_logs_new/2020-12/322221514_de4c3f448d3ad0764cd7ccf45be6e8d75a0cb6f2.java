package com.chess.engine.piece;

import java.util.Collection;
import java.util.List;
import java.util.ArrayList;


import com.chess.engine.board.BoardUtils;
import com.chess.engine.Team;
import com.chess.engine.board.Board;
import com.chess.engine.board.Move;
import com.chess.engine.board.Move.MajorMove;
import com.chess.engine.board.Tile;
import com.google.common.collect.ImmutableList;

public class King extends Piece {
	
	private final static int[] CANDIDATE_MOVE_COORDINATE = {-9, -8, -7, -1, 1, 7, 8, 9};

	public King(final Team pieceTeam, final int piecePosition) {
		super(piecePosition, pieceTeam);
		// TODO Auto-generated constructor stub
	}

	@Override
	public Collection<Move> calculateLegalMoves(Board board) {

		final List<Move> legalMoves = new ArrayList<>();
		
		for(final int currentCandidateOffset : CANDIDATE_MOVE_COORDINATE) {
			
			final int candidateDestinationMove = this.piecePosition + currentCandidateOffset;
			
			if(isFirstColumnExclusion(this.piecePosition,currentCandidateOffset) || 
					isEighthColumnExclusion(this.piecePosition, currentCandidateOffset)) {
				continue;
			}
			
			if(BoardUtils.isValidTileCoordinateDestination(candidateDestinationMove)) {
				final Tile candidateDestinationTile = board.getTile(candidateDestinationMove);
				
				if(!candidateDestinationTile.isTileOccupied()) {
					legalMoves.add(new MajorMove(board,this,candidateDestinationMove));
				} else {
					final Piece pieceAtDestination = candidateDestinationTile.getPiece();
					final Team pieceTeam = pieceAtDestination.getPieceTeam();
					
					if(this.pieceTeam != pieceTeam) {
						legalMoves.add(new Move.AttackMove(board,this,candidateDestinationMove,pieceAtDestination));
					}
				}
			
			}
			
		}
			
			
		return ImmutableList.copyOf(legalMoves);
		
		
	}
	
	private static boolean isFirstColumnExclusion(final int currentPosition, final int candidateOffset) {
		
		return BoardUtils.FIRST_COLUMN[currentPosition] && (candidateOffset == -9 || candidateOffset == -1 ||
			candidateOffset == 7 );		
	
	}
	
	private static boolean isEighthColumnExclusion(final int currentPosition,final int candidateOffset) {
		return BoardUtils.EIGHTH_COLUMN[currentPosition] && (candidateOffset == -7 || candidateOffset == 1 || 
				candidateOffset == 9);
	}

}
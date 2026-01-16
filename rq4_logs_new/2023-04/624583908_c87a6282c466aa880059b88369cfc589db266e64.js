import React from 'react';
import { render, fireEvent } from '@testing-library/react';
import Game from './Game';

describe('Game', () => {
  it('should render the game board', () => {
    const { getByTestId } = render(<Game />);
    expect(getByTestId('game-board')).toBeInTheDocument();
  });

  it('should render the reset button', () => {
    const { getByText } = render(<Game />);
    expect(getByText('Reset Game')).toBeInTheDocument();
  });

  it('should change the player turn on click', () => {
    const { getAllByTestId, getByText } = render(<Game />);
    const squares = getAllByTestId('square');
    const playerTurn = getByText(/next player/i);

    fireEvent.click(squares[0]);
    expect(playerTurn).toHaveTextContent('Next player: O');

    fireEvent.click(squares[1]);
    expect(playerTurn).toHaveTextContent('Next player: X');
  });

  it('should declare a winner when a player wins', () => {
    const { getAllByTestId, getByText } = render(<Game />);
    const squares = getAllByTestId('square');
    const playerTurn = getByText(/next player/i);
    const resetButton = getByText(/reset game/i);

    fireEvent.click(squares[0]); // X
    fireEvent.click(squares[3]); // O
    fireEvent.click(squares[1]); // X
    fireEvent.click(squares[4]); // O
    fireEvent.click(squares[2]); // X

    expect(getByText(/winner:/i)).toBeInTheDocument();
    expect(getByText(/winner:/i)).toHaveTextContent('Winner: X');
    expect(playerTurn).toHaveTextContent('');

    fireEvent.click(resetButton);

    expect(getByText(/next player:/i)).toBeInTheDocument();
    expect(playerTurn).toHaveTextContent('Next player: X');
  });
});
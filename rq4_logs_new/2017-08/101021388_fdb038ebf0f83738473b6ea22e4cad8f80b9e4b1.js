import React from 'react';
import withStyles from 'isomorphic-style-loader/lib/withStyles';
import s from './GameMaze.css';
import gitaUrl from './gita_idle.png';
import MazeData from './MazeData';

class GameMaze extends React.Component {
  constructor(props) {
    super(props);
    const maze = new MazeData();
    this.state = {
      maze,
    };
  }

  renderCell(cell) {
    const myLoc = this.state.maze.myLoc;
    const amHere = cell.x === myLoc.x && cell.y === myLoc.y;
    const cellContents = amHere ? <img src={gitaUrl} alt="Gita" /> : '';

    const mainClass = s.cell;
    const topClass = cell.top ? s.cellTopWall : '';
    const bottomClass = cell.bottom ? s.cellBottomWall : '';
    const leftClass = cell.left ? s.cellLeftWall : '';
    const rightClass = cell.right ? s.cellRightWall : '';
    const classes = `${mainClass} ${topClass} ${bottomClass} ${leftClass} ${rightClass}`;

    return (
      <td className={classes}>
        {cellContents}
      </td>
    );
  }

  render() {
    return (
      <table className={s.table}>
        <tbody>
          {this.state.maze.maze.map(row =>
            <tr>
              {row.map(cell => this.renderCell(cell))}
            </tr>,
          )}
        </tbody>
      </table>
    );
  }
}

export { GameMaze as GameMazeWithoutStyle };
export default withStyles(s)(GameMaze);
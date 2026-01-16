import './StackingCards.css';

function StackingCards() {
  return (
    <div className="StackedCards">
      <div class="header">Header</div>
      <div class="card">
        <img src="https://m.media-amazon.com/images/M/MV5BYjZiMTllNzYtNGQ3Ni00ZWE3LWI1ZjAtZTM5NjFmYTY2ZDY0XkEyXkFqcGdeQXVyMzQ3Nzk5MTU@._V1_.jpg" />
      </div>
      <div class="card">
        <img src="https://m.media-amazon.com/images/M/MV5BMjUxMjE4MTQxMF5BMl5BanBnXkFtZTcwNzc2MDM1NA@@._V1_.jpg"/>
      </div>
      <div class="card">
        <img src ="https://pyxis.nymag.com/v1/imgs/a6d/bf9/5892e8e7752075fed0ac7a37912e335814-nicolas-cage.jpg" />
      </div>
      <div class="footer">Footer</div>
    </div>
  );
}

export default StackingCards;
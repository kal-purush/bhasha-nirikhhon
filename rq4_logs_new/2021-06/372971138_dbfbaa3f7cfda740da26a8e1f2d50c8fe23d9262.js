import EventParser, { matches } from './parser';

const parser = new EventParser();

matches.forEach(match => {
    const matchContainerEl = document.createElement('div');

    const matchNameEl = document.createElement('span');
    matchNameEl.className = 'name';
    matchNameEl.textContent = parser.makeEventName(match);

    const matchScoreEl = document.createElement('span');
    matchScoreEl.className = 'score';
    matchScoreEl.textContent = parser.formatScore(match);

    matchContainerEl.appendChild(matchNameEl);
    matchContainerEl.appendChild(matchScoreEl);

    document.body.appendChild(matchContainerEl);
});

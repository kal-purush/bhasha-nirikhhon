// ========== Constantes ==========
const rankings = [
    { min: 1, name: 'Iniciante' },
    { min: 10, name: 'Aprendiz' },
    { min: 20, name: 'Veterano' },
    { min: 30, name: 'Especialista' },
    { min: 40, name: 'Mestre' },
    { min: 50, name: 'Lenda' }
  ];
  
  // ========== Estado Inicial ==========
  let state = JSON.parse(localStorage.getItem('gameState')) || { level:1, xp:0 };
  let customMissions = JSON.parse(localStorage.getItem('customMissions') || '[]');
  let history = JSON.parse(localStorage.getItem('taskHistory') || '[]');
  let breakInterval;
  
  // ========== Funções ==========
  function save() {
    localStorage.setItem('gameState', JSON.stringify(state));
    localStorage.setItem('customMissions', JSON.stringify(customMissions));
    localStorage.setItem('taskHistory', JSON.stringify(history));
  }
  
  function updateStats(){
    // Lógica de level-up
    let xpToNext = state.level * 100;
    while(state.xp >= xpToNext) {
      state.xp -= xpToNext;
      state.level++;
      xpToNext = state.level * 100;
      alert(`🎉 Parabéns! Você alcançou o nível ${state.level}!`);
    }
    
    // Atualiza UI
    document.getElementById('level').innerText = state.level;
    document.getElementById('xp').innerText = state.xp;
    document.getElementById('xpToNext').innerText = xpToNext;
    document.getElementById('progress').style.width = `${Math.min(100, (state.xp / xpToNext) * 100)}%`;
  
    const rank = rankings.slice().reverse().find(r => state.level >= r.min);
    document.getElementById('levelName').innerText = `Nível ${state.level}: ${rank.name}`;
  }
  
  function renderHistory(){
    const tb = document.getElementById('history'); tb.innerHTML = '';
    history.forEach(e => {
      const tr = document.createElement('tr');
      tr.innerHTML = `<td>${e.date}</td><td>${e.desc}</td><td>${e.xp}</td>`;
      tb.appendChild(tr);
    });
  }
  
  function renderMissions(){
    const ul = document.getElementById('missions'); ul.innerHTML = '';
    customMissions.forEach((m, i) => {
      const li = document.createElement('li');
      li.className = 'mission';
      li.style.borderLeftColor = m.color;
      li.innerHTML = `
        <div class='mission-header'><strong>${m.desc}</strong><div>
          <button class='completeM' data-i='${i}'>+1</button>
          <button class='editM' data-i='${i}'>Editar</button>
          <button class='deleteM' data-i='${i}'>X</button>
        </div></div>
        <div class='mission-details'>Dificuldade: ${m.difficulty} | Tempo: ${m.time}min | Progresso: ${m.count}/${m.goal} | Recompensa: ${m.reward}</div>
        <div class='progress-mission'><div class='progress-bar-m' style='width:${(m.count/m.goal)*100}%'></div></div>
      `;
      ul.appendChild(li);
    });
  }
  
  function startBreak(mins){
    let sec = mins * 60;
    document.getElementById('breakOverlay').style.display = 'flex';
    breakInterval = setInterval(()=>{
      const m = Math.floor(sec/60), s = sec%60;
      document.getElementById('breakTimer').innerText = `${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')}`;
      if(sec-- <= 0){ clearInterval(breakInterval); document.getElementById('breakOverlay').style.display = 'none'; }
    },1000);
    document.getElementById('cancelBreak').onclick = ()=>{ clearInterval(breakInterval); document.getElementById('breakOverlay').style.display = 'none'; };
  }
  
  // ========== Eventos ==========
  document.getElementById('showAddMission').onclick = ()=>{
    const form = document.getElementById('addMissionForm');
    form.style.display = form.style.display === 'none' ? 'block' : 'none';
  };
  
  document.getElementById('addMissionForm').onsubmit = e => {
    e.preventDefault();
    const m = {
      desc: mDesc.value,
      color: mColor.value,
      difficulty: +mDifficulty.value,
      time: +mTime.value,
      goal: +mGoal.value,
      reward: mReward.value,
      break: +mBreak.value,
      count: 0
    };
    customMissions.push(m);
    save();
    renderMissions();
    e.target.reset();
  };
  
  document.getElementById('missions').onclick = e => {
    const i = +e.target.dataset.i;
    if(e.target.classList.contains('completeM')){
      const m = customMissions[i]; m.count++;
      state.xp += m.difficulty * 10;
      history.unshift({ date: new Date().toLocaleString(), desc: m.desc, xp: m.difficulty * 10 });
      save(); updateStats(); renderHistory(); renderMissions();
      if(m.break > 0) startBreak(m.break);
    }
    if(e.target.classList.contains('editM')){
      const m = customMissions[i];
      const newGoal = +prompt('Nova meta:', m.goal);
      const newReward = prompt('Nova recompensa:', m.reward);
      if(newGoal > 0) m.goal = newGoal;
      if(newReward) m.reward = newReward;
      save(); renderMissions();
    }
    if(e.target.classList.contains('deleteM')){
      customMissions.splice(i,1);
      save(); renderMissions();
    }
  };
  
  document.getElementById('resetBtn').onclick = ()=>{
    if(confirm('Tem certeza que deseja resetar tudo?')){
      localStorage.clear();
      state = {level:1, xp:0};
      customMissions = [];
      history = [];
      save();
      updateStats();
      renderHistory();
      renderMissions();
    }
  };
  
  // ========== Inicialização ==========
  updateStats();
  renderHistory();
  renderMissions();
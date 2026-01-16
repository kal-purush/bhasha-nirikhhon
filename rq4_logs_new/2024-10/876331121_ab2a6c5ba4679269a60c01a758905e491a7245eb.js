let pingIntervals = {};
let lastPingTimes = {}; // Armazenar o último tempo de ping de cada host

// Função para carregar hosts do localStorage
function loadHosts() {
  const storedHosts = JSON.parse(localStorage.getItem('hosts')) || [];
  storedHosts.forEach(({ networkName, host }) => {
    addHostToList(networkName, host); // Adiciona cada host armazenado na lista
    pingNode(host); // Inicia o ping para cada host já salvo
    pingIntervals[host] = setInterval(() => pingNode(host), 10000); // Inicia o intervalo de ping
  });
}

// Função para adicionar um host à lista como um card
function addHostToList(networkName, host) {
  const hostList = document.getElementById('hostList');
  const colDiv = document.createElement('div');
  colDiv.className = "col-lg-2 mb-3"; // Coluna responsiva
  
  colDiv.innerHTML = `
    <div class="card">
      <div class="card-body bg-success text-light">
        <h5 class="card-title">${networkName}</h5>
        <h6 class="card-subtitle mb-2 ">${host}</h6>
        <p class="card-text"><span id="status-${host}">Esperando...</span></p>
        <button onclick="removeHost('${host}', this)" class="d-none btn btn-danger">Remover</button>
      </div>
    </div>
  `;
  hostList.appendChild(colDiv);
}


// Adiciona um novo host
function addHost() {
  const networkNameInput = document.getElementById('networkName');
  const hostInput = document.getElementById('host');
  
  const networkName = networkNameInput.value.trim();
  const host = hostInput.value.trim();

  if (!networkName || !host) {
    alert('Por favor, insira o nome da rede e o host.');
    return;
  }

  addHostToList(networkName, host); // Adiciona o host à lista
  pingNode(host); // Inicia o ping para o novo host
  pingIntervals[host] = setInterval(() => pingNode(host), 10000); // Inicia o intervalo de ping

  // Salva o host no localStorage
  saveHostToLocalStorage(networkName, host);

  // Limpa os campos de entrada
  networkNameInput.value = '';
  hostInput.value = '';
}


// Função para salvar o host no localStorage
function saveHostToLocalStorage(networkName, host) {
  const storedHosts = JSON.parse(localStorage.getItem('hosts')) || [];
  storedHosts.push({ networkName, host });
  localStorage.setItem('hosts', JSON.stringify(storedHosts));
}

// Função para fazer o ping
function pingNode(host) {
  const currentTime = Date.now();

  // Verifica se já passaram 60 segundos desde o último ping
  if (lastPingTimes[host] && currentTime - lastPingTimes[host] < 60000) {
    console.log(`Cooldown em vigor para ${host}. Esperando...`);
    return; // Sai da função se estiver dentro do cooldown
  }

  // Faz a requisição para o backend para verificar o ping
  fetch(`http://localhost:3000/ping/${host}`)
    .then(response => {
      // Verifica se a resposta é bem-sucedida
      if (!response.ok) {
        throw new Error('Erro na resposta do servidor');
      }
      return response.json();
    })
    .then(data => {
      const statusElement = document.getElementById(`status-${host}`);
      const cardBody = statusElement.closest('.card-body'); // Acessa o card-body mais próximo
      const result = data.alive 
        ? `ONLINE | Latência: ${data.time} ms` 
        : `OFFLINE | OFFLINE`;
      
      statusElement.innerText = result;
      console.log(result);
  
      // Atualiza a classe do card-body com base no status
      if (data.alive) {
        cardBody.classList.remove('bg-danger');
        cardBody.classList.add('bg-success');
      } else {
        cardBody.classList.remove('bg-success');
        cardBody.classList.add('bg-danger');
      }

      // Atualiza o último tempo de ping
      lastPingTimes[host] = Date.now();
    })
    .catch(error => {
      const statusElement = document.getElementById(`status-${host}`);
      statusElement.innerText = 'Erro ao fazer ping';
      console.error('Erro:', error); // Log do erro no console
    });
}


// Função para remover um host
function removeHost(host, button) {
  // Remove o host da lista
  const hostList = document.getElementById('hostList');
  hostList.removeChild(button.parentNode);

  // Para o intervalo de ping
  clearInterval(pingIntervals[host]);
  delete pingIntervals[host];
  delete lastPingTimes[host]; // Remove o tempo do último ping

  // Remove o host do localStorage
  removeHostFromLocalStorage(host);
}

// Função para remover o host do localStorage
function removeHostFromLocalStorage(host) {
  const storedHosts = JSON.parse(localStorage.getItem('hosts')) || [];
  const updatedHosts = storedHosts.filter(h => h.host !== host);
  localStorage.setItem('hosts', JSON.stringify(updatedHosts));
}

// Função para parar todos os pings
function stopPings() {
  for (const host in pingIntervals) {
    clearInterval(pingIntervals[host]);
    delete pingIntervals[host];
    delete lastPingTimes[host]; // Remove o tempo do último ping
  }
  document.getElementById('result').innerText = 'Todos os pings parados.';
}

// Carrega os hosts quando a página é carregada
window.onload = loadHosts;
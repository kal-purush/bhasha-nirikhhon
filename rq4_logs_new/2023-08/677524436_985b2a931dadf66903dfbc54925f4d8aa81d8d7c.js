const { ipcRenderer } = require('electron');

document.getElementById('buttonInMainWindow').addEventListener('click', () => {
  ipcRenderer.send('button-clicked', 'Bouton cliqué dans la première fenêtre');
});
let data = JSON.parse(localStorage.getItem('favsData')) || {
  layouts: [{ name: 'Default', links: [] }],
  currentLayout: 0,
  lastRemoved: null
};

const cardsContainer = document.getElementById('cardsContainer');
const layoutTabs = document.getElementById('layoutTabs');
const snackbar = document.getElementById('snackbar');
const undoBtn = document.getElementById('undoBtn');
const searchInput = document.getElementById('searchInput');
const importFile = document.getElementById('importFile');
const fab = document.getElementById('fab');

let removeMode = false;

function save() {
  localStorage.setItem('favsData', JSON.stringify(data));
}

function renderTabs() {
  layoutTabs.innerHTML = '';
  data.layouts.forEach((lyt, idx) => {
    const btn = document.createElement('button');
    btn.textContent = lyt.name;
    btn.classList.toggle('active', idx === data.currentLayout);
    btn.onclick = () => {
      data.currentLayout = idx;
      save();
      render();
    };
    layoutTabs.appendChild(btn);
  });
}

function renderCards() {
  cardsContainer.innerHTML = '';
  let list = data.layouts[data.currentLayout].links;

  const q = searchInput.value.trim().toLowerCase();
  if (q) {
    list = list.filter(l =>
      l.name.toLowerCase().includes(q) ||
      l.url.toLowerCase().includes(q)
    );
  }

  list.forEach((link, idx) => {
    const card = document.createElement('div');
    card.className = 'card';
    card.innerHTML = `
      <button class="remove-btn" title="Remove this website">
        <i class="fas fa-times" aria-hidden="true"></i>
      </button>
      <strong>${link.name}</strong>
      <a href="${link.url}" target="_blank" rel="noopener noreferrer">
        ${link.url.replace(/^https?:\/\//, '')}
      </a>
    `;
    card.querySelector('.remove-btn').onclick = () => removeLink(idx);
    cardsContainer.appendChild(card);
  });

  Sortable.create(cardsContainer, {
    animation: 150,
    onEnd: evt => {
      const arr = data.layouts[data.currentLayout].links;
      arr.splice(evt.newIndex, 0, arr.splice(evt.oldIndex, 1)[0]);
      save();
    }
  });
}

function removeLink(idx) {
  const arr = data.layouts[data.currentLayout].links;
  data.lastRemoved = { layout: data.currentLayout, item: arr[idx], index: idx };
  arr.splice(idx, 1);
  save();
  render();
  showSnackbar();
}

function showSnackbar() {
  snackbar.classList.add('show');
  setTimeout(() => snackbar.classList.remove('show'), 5000);
}

undoBtn.onclick = () => {
  if (data.lastRemoved) {
    const { layout, item, index } = data.lastRemoved;
    data.layouts[layout].links.splice(index, 0, item);
    data.lastRemoved = null;
    save();
    render();
  }
};

function render() {
  document.body.classList.toggle('remove-mode', removeMode);
  renderTabs();
  renderCards();
}

searchInput.oninput = render;

document.getElementById('fabMain').onclick = () => {
  fab.classList.toggle('open');
};

function toggleModal(id) {
  document.getElementById(id).classList.toggle('active');
}

document.getElementById('addSiteBtn').onclick = () => toggleModal('siteModal');
document.getElementById('toggleRemoveBtn').onclick = () => {
  removeMode = !removeMode;
  render();
};
document.getElementById('addLayoutBtn').onclick = () => toggleModal('layoutModal');
document.getElementById('themeMenuBtn').onclick = () => {
  const newTheme = document.body.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
  document.body.setAttribute('data-theme', newTheme);
};

document.querySelectorAll('.modal .close').forEach(btn =>
  btn.onclick = () => toggleModal(btn.dataset.close)
);

document.getElementById('siteForm').onsubmit = e => {
  e.preventDefault();
  const name = e.target.siteName.value.trim();
  const url = e.target.siteURL.value.trim();
  if (name && url) {
    data.layouts[data.currentLayout].links.unshift({ name, url });
    save();
    render();
    toggleModal('siteModal');
    e.target.reset();
  }
};

document.getElementById('layoutForm').onsubmit = e => {
  e.preventDefault();
  const name = e.target.layoutName.value.trim();
  if (name) {
    data.layouts.push({ name, links: [] });
    data.currentLayout = data.layouts.length - 1;
    save();
    render();
    toggleModal('layoutModal');
    e.target.reset();
  }
};

document.getElementById('exportBtn').onclick = () => {
  const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = 'fav-webs-data.json';
  a.click();
  URL.revokeObjectURL(url);
};

document.getElementById('importBtn').onclick = () => importFile.click();
importFile.onchange = e => {
  const file = e.target.files[0];
  if (!file) return;
  const reader = new FileReader();
  reader.onload = ev => {
    try {
      data = JSON.parse(ev.target.result);
      save();
      render();
    } catch {
      alert('Invalid JSON file');
    }
  };
  reader.readAsText(file);
};

render();
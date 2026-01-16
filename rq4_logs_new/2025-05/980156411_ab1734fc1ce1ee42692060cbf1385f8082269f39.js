document.addEventListener('DOMContentLoaded', () => {
    const modal = document.getElementById('modal');
    const btnAbrirModalAdc = document.getElementById('abrir-adc');
    const btnFecharModal = document.getElementById('fechar-modal');
    const tipoSelect = document.getElementById('tipo');
    const categoriaSelect = document.getElementById('categoria');
    const form = document.getElementById('form-transacao');
    const tabela = document.getElementById('tabela-transacoes');
  
    // Modal exclusão
    const modalExcluir = document.getElementById('modal-excluir');
    const btnFecharModalExcluir = document.getElementById('fechar-modal-excluir');
    const btnConfirmarExclusao = document.getElementById('confirmar-exclusao');
    let idParaExcluir = null;
  
    btnAbrirModalAdc.addEventListener('click', () => {
      modal.classList.remove('hidden');
    });
  
    btnFecharModal.addEventListener('click', () => {
      modal.classList.add('hidden');
      form.reset();
      categoriaSelect.innerHTML = '<option value="" selected disabled hidden>Selecione o tipo primeiro</option>';
      categoriaSelect.disabled = true;
    });
  
    btnFecharModalExcluir.addEventListener('click', () => {
      modalExcluir.classList.add('hidden');
      idParaExcluir = null;
    });
  
    btnConfirmarExclusao.addEventListener('click', async () => {
      if (idParaExcluir) {
        await fetch(`/transacoes/${idParaExcluir}`, { method: 'DELETE' });
        idParaExcluir = null;
        modalExcluir.classList.add('hidden');
        carregarTransacoes();
        atualizarResumo();
      }
    });
  
    const carregarTransacoes = async () => {
      const res = await fetch('/transacoes');
      const transacoes = await res.json();
  
      tabela.innerHTML = '';
      transacoes.forEach(t => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td>${new Date(t.data).toLocaleDateString()}</td>
          <td>${t.tipo}</td>
          <td>${t.categoria}</td>
          <td>${t.descricao}</td>
          <td>R$ ${t.valor.toFixed(2)}</td>
          <td><button class="excluir-btn" data-id="${t.id}">X</button></td>
        `;
        tabela.appendChild(tr);
      });
  
      adicionarEventosExclusao();
    };
  
    const adicionarEventosExclusao = () => {
      document.querySelectorAll('.excluir-btn').forEach(btn => {
        btn.addEventListener('click', () => {
          idParaExcluir = btn.getAttribute('data-id');
          modalExcluir.classList.remove('hidden');
        });
      });
    };
  
    const atualizarResumo = async () => {
      const res = await fetch('/lucro');
      const dados = await res.json();
  
      document.getElementById('resumo-entradas').textContent = `Entradas: R$ ${dados.totalEntradas.toFixed(2)}`;
      document.getElementById('resumo-saidas').textContent = `Saídas: R$ ${dados.totalSaidas.toFixed(2)}`;
      document.getElementById('resumo-lucro').textContent = `Lucro: R$ ${dados.lucro.toFixed(2)}`;
    };
  
    tipoSelect.addEventListener('change', async () => {
      const tipo = tipoSelect.value;
      categoriaSelect.disabled = true;
      categoriaSelect.innerHTML = '<option value="" selected disabled hidden>Carregando...</option>';
  
      try {
        const res = await fetch(`/categorias?tipo=${tipo}`);
        const categorias = await res.json();
  
        categoriaSelect.innerHTML = '<option value="" selected disabled hidden>Selecione...</option>';
        categorias.forEach(cat => {
          const option = document.createElement('option');
          option.value = cat;
          option.textContent = cat;
          categoriaSelect.appendChild(option);
        });
        categoriaSelect.disabled = false;
      } catch (error) {
        categoriaSelect.innerHTML = '<option value="" selected disabled hidden>Erro ao carregar</option>';
      }
    });
  
    form.addEventListener('submit', async (e) => {
      e.preventDefault();
  
      const tipo = tipoSelect.value;
      const categoria = categoriaSelect.value;
      const valor = parseFloat(document.getElementById('valor').value);
      const descricao = document.getElementById('descricao').value;
  
      const transacao = { valor, descricao, categoria };
  
      await fetch(`/${tipo}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(transacao)
      });
  
      form.reset();
      categoriaSelect.disabled = true;
      categoriaSelect.innerHTML = '<option value="" selected disabled hidden>Selecione o tipo primeiro</option>';
      modal.classList.add('hidden');
  
      carregarTransacoes();
      atualizarResumo();
    });
  
    carregarTransacoes();
    atualizarResumo();
  });  
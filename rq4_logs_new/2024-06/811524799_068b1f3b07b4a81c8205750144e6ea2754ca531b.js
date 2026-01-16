import React, { useState, useEffect } from 'react';

import TotalVendas from '../TotalVendas';
import api from '../../services/api';
import './style.css'
import Filtro from '../Filtro';

export default function Status() {
  const [resumoStatus, setResumoStatus] = useState(null);

  useEffect(() => {
    const fetchResumoStatus = async () => {
      try {
        const response = await api.get('/pedidos/resumoStatus');
        setResumoStatus(response.data);
      } catch (error) {
        console.error('Erro ao buscar resumo de status:', error);
      }
    };

    fetchResumoStatus();
  }, []);


return (
  <div className='status'>
    {resumoStatus && (
      <div className='status-pedido box-shadow'>
        <h1>Status dos Pedidos</h1>
        <div className='dados-pedidos'>
          <div className='dados'>
            <span className='processando'>{resumoStatus.qtdStatusProcessando}</span>
            <p>Processando</p>
          </div>
          <div className='dados'>
            <span className='pendente'>{resumoStatus.qtdStatusPendente}</span>
            <p>Pendente</p>
          </div>
          <div className='dados'>
            <span className='aprovado'>{resumoStatus.qtdStatusAprovado}</span>
            <p>Aprovado</p>
          </div>
          <div className='dados'>
            <span className='cancelado'>{resumoStatus.qtdStatusCancelado}</span>
            <p>Cancelado</p>
          </div>
          <div className='dados'>
            <span className='total'>{resumoStatus.qtdTotalPedidos}</span>
            <p>Total</p>
          </div>
        </div>
      </div>
    )}

    <Filtro />


    <TotalVendas />
  </div>
);

}
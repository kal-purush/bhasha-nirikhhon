import request from 'supertest';
import app from '../../index.js';
import Automovel from "../../models/automovel.js";

describe('Testes da Rota /create-car', () => {
  // Teste para cadastrar um novo automóvel
  it('Deve cadastrar um novo automóvel e retornar status 201', async () => {
    const novoAutomovel = {
      placa: 'ABC1234',
      marca: 'Ford',
      cor: 'Azul',
    };

    const response = await request(app)
      .post('/create-car')
      .send(novoAutomovel);

    expect(response.status).toBe(201);
    expect(response.body).toHaveProperty('AutomovelId'); // Verifica se o AutomovelId está presente na resposta
    expect(response.body).toMatchObject({
      ...novoAutomovel,
      AutomovelId: expect.any(Number),
    });

    // Verificar se o automóvel foi realmente salvo no banco de dados
    const automovelSalvo = await Automovel.findByPk(response.body.AutomovelId);
    expect(automovelSalvo).toBeTruthy();
    expect(automovelSalvo.placa).toBe(novoAutomovel.placa);
    expect(automovelSalvo.marca).toBe(novoAutomovel.marca);
    expect(automovelSalvo.cor).toBe(novoAutomovel.cor);
  });

  // Teste para validar se a placa é obrigatória
  it('Deve retornar status 400 se não fornecer placa', async () => {
    const automovelSemPlaca = {
      marca: 'Ford',
      cor: 'Azul',
    };

    const response = await request(app)
      .post('/create-car')
      .send(automovelSemPlaca);

    expect(response.status).toBe(400);
    expect(response.body.error).toBeTruthy();
  });
});

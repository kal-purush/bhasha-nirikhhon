import { Router } from 'express';
import { RestauranteController } from '../controllers/restaurante.controller';
import { ProdutoController } from '../controllers/produto.controller';

const produtoRoute = Router();
const controller = new ProdutoController();

produtoRoute.get('/produto', async function (request, response) {
  await response.json(await controller.all());
});


produtoRoute.get('/produto/:id', async function (request, response) {
  await response.json(await controller.get(Number(request.params.id)));
});

produtoRoute.post('/produto/create', async function (request, response) {
  await response.json(await controller.create(await request.body));
});


produtoRoute.put('/produto/edit/:id', async function (request, response) {
  await response.json(await controller.update(Number(request.params.id), await request.body));
});

produtoRoute.delete('/produto/delete/:id', async function (request, response) {
  await response.json(await controller.destroy(Number(request.params.id)));
});

export default produtoRoute;

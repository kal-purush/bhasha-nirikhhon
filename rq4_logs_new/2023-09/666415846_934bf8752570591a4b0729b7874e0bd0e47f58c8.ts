import dotenv from 'dotenv';
dotenv.config();

import express, { Request } from 'express';
import path from 'path';
import fs from 'fs/promises';
import { createServer as createViteServer } from 'vite';
import type { ViteDevServer } from 'vite';
import cookieParser from 'cookie-parser';
import serialize from 'serialize-javascript';

const port = process.env.CLIENT_PORT || 80;
const clientPath = path.join(__dirname, '..');
const isDev = process.env.NODE_ENV === 'development';

async function createServer() {
	const app = express();

	app.use(cookieParser());
	let vite: ViteDevServer | undefined;
	if (isDev) {
		vite = await createViteServer({
			server: { middlewareMode: true },
			root: clientPath,
			appType: 'custom',
		});

		app.use(vite.middlewares);
	} else {
		app.use(
			express.static(path.join(clientPath, 'dist/client'), { index: false }),
		);
	}

	app.get('*', async (req, res, next) => {
		const url = req.originalUrl;

		try {
			let render: (
				req: Request
			) => Promise<{ html: string; initialState: unknown }>;
			let template: string;

			if (vite) {
				// Получаем файл client/index.html который мы правили ранее
				template = await fs.readFile(
					path.resolve(clientPath, 'index.html'),
					'utf-8',
				);

				// Применяем встроенные HTML-преобразования vite и плагинов
				template = await vite.transformIndexHtml(url, template);

				// Загружаем модуль, который будет рендерить нам HTML-код
				render = (
					await vite.ssrLoadModule(
						path.join(clientPath, 'src/entry-server.tsx'),
					)
				).render;
			} else {
				template = await fs.readFile(
					path.join(clientPath, 'dist/client/index.html'),
					'utf-8',
				);

				// Получаем путь до сбилдженого модуля клиента, чтобы не тащить средства сборки клиента на сервер
				const pathToServer = path.join(
					clientPath,
					'dist/server/entry-server.js',
				);

				// Импортируем этот модуль и вызываем с инишл стейтом
				render = (await import(pathToServer)).render;
			}

			// Получаем HTML-строку из JSX
			const { html: appHtml, initialState } = await render(req);

			// Заменяем наш комментарий на сгенерированную HTML-строку
			const html = template.replace('<!--ssr-outlet-->', appHtml).replace(
				'<!--ssr-initial-state-->',
				`<script>window.APP_INITIAL_STATE = ${serialize(initialState, {
					isJSON: true,
				})}</script>`,
			);

			// Завершаем запрос и отдаем HTML-страницу
			res.status(200).set({ 'Content-Type': 'text/html' }).end(html);
		} catch (e) {
			if (vite) {
				vite.ssrFixStacktrace(e as Error);
			}
			next(e);
		}
	});

	app.listen(port, () => {
		console.log(`Client is listening on port: ${port}`);
	});
}

createServer();
import * as express from 'express';
import {resolve, join} from 'path';
import {root, port, dist} from './config/config';
import {isFileExt} from './config/utils';

const app = express();
const staticPath = resolve(root, 'app');

app.set('view engine', 'ejs');
app.set('views', resolve(__dirname, 'views'));
app.get('/*', (req: express.Request, res: express.Response, next: Function) => {
	if (isFileExt(req.url)) {
		next();
		return;
	}
	res.render('index');
});
app.use(express.static(join(__dirname, '../','/app/dist')));
app.listen(port);
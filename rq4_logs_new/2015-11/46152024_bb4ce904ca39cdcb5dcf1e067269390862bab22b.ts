import { provide, Injectable, Observable } from 'angular2/angular2';
import { Http, Response, HTTP_PROVIDERS } from 'angular2/http';
import { ReplaySubject } from '@reactivex/rxjs';
import Gasket from '../models/Gasket';
import GasketService from './GasketService';

@Injectable()
export default class HttpGasketService extends GasketService {
	private http: Http;

	constructor(http: Http) {
		super();

		this.http = http;
	}

	getGaskets(): Observable<Gasket[]> {
		return this.get('dist/app/fixtures/gaskets.json');
	}

	getGasket(index: number | string): Observable<Gasket> {
		return this.get(`dist/app/fixtures/gaskets/${index}.json`);
	}

	private get<T>(url: string): Observable<T> {
		const o = new ReplaySubject<T>(1);

		this.http.get(url)
			.map((res: Response) => res.json())
			.subscribe((object: T) => {
				o.next(object);
			})
		;

		return o;
	}
}

export const GASKET_SERVICE_PROVIDER = [
	HTTP_PROVIDERS,
	provide(GasketService, { useClass: HttpGasketService })
];
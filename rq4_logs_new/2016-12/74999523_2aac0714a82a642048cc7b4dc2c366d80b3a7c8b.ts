import { Observable, Observer } from 'rxjs/Rx';
import { Injectable } from '@angular/core';

@Injectable()
export class PostsService {
	public getPosts(): Observable<any[]> {
		return new Observable<any[]>((obs: Observer<any[]>) => {
			let data: any[] = [];
			for(let i=0; i<15; i++) {
				data.push({ titulo: 'Post - ' + i, categoria: 'Angulando', usuarios: 'Juan Perez - Rosa Garcia - Luis Morales', respuestas: 5, vistas: 254, actividad: new Date(2016, 11, 11, 4, 35, 21) })
			}
			obs.next(data);
			obs.complete();

		});
	}
}
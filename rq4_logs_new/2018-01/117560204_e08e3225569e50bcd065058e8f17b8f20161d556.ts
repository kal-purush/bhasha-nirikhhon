import { Component, OnInit, Input } from '@angular/core';

@Component({
	selector: 'app-game-score',
	templateUrl: './score.component.html',
	styleUrls: ['./score.component.scss']
})
export class ScoreComponent implements OnInit {
	@Input() length: number;
	@Input() value: any;
	@Input() btnClass: String;

	ngOnInit(): void {
		if (this.length === undefined) { this.length = 3; }
		if (this.value === undefined) { this.value = 0; }
	}

	private _pad(n: String, width: number, z: String = '0'): String {
		n += '';
		return n.length >= width ? n : new Array<String>(width - n.length + 1).join(z.toString()) + n;
	}

	get padValue(): String {
		return this._pad(this.value, this.length);
	}

	get splitValue(): String[] {
		const list = new Array<String>();
		const attr = 1 / this.length;
		for (let i = 0; i < this.length; i++) {
			list.push(this.padValue.substring(Math.floor(this.length * attr * i), Math.floor(this.length * attr * (i + 1))));
		}
		return list;
	}
}
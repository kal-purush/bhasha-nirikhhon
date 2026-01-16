import { ChangeDetectionStrategy, Component, EventEmitter, OnDestroy, OnInit, Output } from '@angular/core';
import { FormControl } from '@angular/forms';
import { Mode } from '../../interfaces';
import { Subject } from 'rxjs';

@Component({
	selector: 'bp-change-mode',
	templateUrl: './change-mode.component.html',
	styleUrls: ['./change-mode.component.scss'],
	changeDetection: ChangeDetectionStrategy.OnPush,
})
export class ChangeModeComponent implements OnInit, OnDestroy {
	readonly changeMode = new FormControl();
	@Output() mode = new EventEmitter<Mode>();

	private readonly _destroy$ = new Subject<boolean>();

	ngOnInit() {
		this.changeMode.valueChanges.subscribe(mode => {
			this.mode.emit(mode);
		});

		this.changeMode.setValue('daily');
	}
	ngOnDestroy() {
		this._destroy$.next(true);
		this._destroy$.complete();
	}
}
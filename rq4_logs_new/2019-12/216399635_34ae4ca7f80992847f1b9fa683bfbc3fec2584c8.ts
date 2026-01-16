import { ChangeDetectionStrategy, Component, Input } from '@angular/core';

import { SpinnerPosition } from '../../types/spinner-position';
import { SpinnerSize } from '../../types/spinner-size';

@Component({
    selector: 'ct-spinner',
    templateUrl: './spinner.component.html',
    styleUrls: ['./spinner.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush
})
export class SpinnerComponent {
    @Input() position: SpinnerPosition = SpinnerPosition.ContainerCentered;
    @Input() size: SpinnerSize = SpinnerSize.Normal;
}
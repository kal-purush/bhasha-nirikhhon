import {MozServiceDataLoad} from './moz-service.data.load';
import {MozLayoutAnimations} from '../class/moz-animations';

export class MozDomManipulator extends MozServiceDataLoad {

    constructor() {
        super();
    }

    public getGridStyle() {
        const style = {};

        style['display'] = 'grid';

        style['grid-template-areas'] = this.generateGridTemplate();

        style['grid-template-rows'] = [
            this.currentLayoutSize.TH + 'px',
            this.currentLayoutSize.MH + 'px',
            this.currentLayoutSize.BH + 'px',
            'auto',
            this.currentLayoutSize.TF + 'px',
            this.currentLayoutSize.MF + 'px',
            this.currentLayoutSize.BF + 'px',
        ].join(' ');

        style['grid-template-columns'] = [
            this.currentLayoutSize.LS + 'px',
            this.currentLayoutSize.LC + 'px',
            'auto',
            this.currentLayoutSize.RC + 'px',
            this.currentLayoutSize.RS + 'px',
        ].join(' ');

        return style;
    }

    public setLayoutAreaSize(areaKey: string, value: number) {
        this.isValidArea(areaKey);
        const animation = new MozLayoutAnimations(this.currentLayoutSize[areaKey], value);
        animation.animate().subscribe((newValue: number) => {
            this.getAreaBehaviorSubject(areaKey).next(this.getAreaStateNameByValue(areaKey, newValue, 'transition'));
            this.currentLayoutSize[areaKey] = newValue;
        }, () => {
        }, () => {
        });
    }

    private generateGridTemplate(): string {
        const gridTemplate = [
            'TH TH TH TH TH',
            'MH MH MH MH MH',
            'BH BH BH BH BH',
            'LS LC MC RC RS',
            'TF TF TF TF TF',
            'MF MF MF MF MF',
            'BF BF BF BF BF'
        ];

        return gridTemplate.reduce((current: string, row: string) => {
            current += '"' + row + '"';
            return current;
        }, '');
    }

}
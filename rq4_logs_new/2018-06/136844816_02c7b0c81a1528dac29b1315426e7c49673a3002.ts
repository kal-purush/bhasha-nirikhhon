import {Observer} from '../core/observer';
import {PlotView} from '../views/plot_view';

export class PlotController extends Observer {
    private view: PlotView = new PlotView();
}
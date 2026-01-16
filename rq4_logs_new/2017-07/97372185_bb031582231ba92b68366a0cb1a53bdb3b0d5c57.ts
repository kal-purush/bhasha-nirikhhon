import {
  AfterContentInit, Directive, EventEmitter, Input, OnChanges, OnDestroy,
  Output, SimpleChange
} from '@angular/core';
import {MarkerLayerManager} from "../../../services/marker-layer-manager.service";

@Directive({
  selector: '[ESPMarker]'
})
export class ESPMarkerDirective implements OnChanges, OnDestroy{

  /**
   * The lat position of the marker.
   */
  @Input() lat: number;

  /**
   * The lng position of the marker.
   */
  @Input() lng: number;

  /**
   * The title of the marker.
   */
  @Input() title: string;

  /**
   * The label (a single uppercase character) for the marker.
   */
  @Input() label: string;

  /**
   * Icon (the URL of the image) for the foreground.
   */
  @Input() iconUrl: string;


  /**
   * The icon size.
   */
  @Input() iconSize: string;

  /**
   * If true, the marker is visible
   */
  @Input() visible: boolean = true;

  /**
   * If true, the marker can be clicked. Default value is true.
   */
    // tslint:disable-next-line:no-input-rename
  @Input('markerClickable') clickable: boolean = true;

  /**
   * This event emitter gets emitted when the user clicks on the marker.
   */
  @Output() markerClick: EventEmitter<void> = new EventEmitter<void>();

  private _markerAddedToManger: boolean = false;

  // This will be initialized when the marker is registered !
  _id: string;
  constructor(private _markerLayerManager: MarkerLayerManager) {}

  /** @internal */
  ngOnChanges(changes: {[key: string]: SimpleChange}) {
    if (typeof this.lat !== 'number' || typeof this.lng !== 'number') {
      console.error('marker lat and lng must be numbers.')
      return;
    }
    if (!this._markerAddedToManger) {
      this._markerLayerManager.addMarker(this);
      debugger;

      this._markerAddedToManger = true;
      return;
    }
    // if (changes['lat'] || changes['lng']) {
    //   this._markerLayerManager.updateMarkerPosition(this);
    // }
    // if (changes['title']) {
    //   this._markerLayerManager.updateTitle(this);
    // }
    // if (changes['label']) {
    //   this._markerLayerManager.updateLabel(this);
    // }
    // if (changes['iconUrl']) {
    //   this._markerLayerManager.updateIcon(this);
    // }
    // if (changes['visible']) {
    //   this._markerLayerManager.updateVisible(this);
    // }
    // if (changes['clickable']) {
    //   this._markerLayerManager.updateClickable(this);
    // }
  }

  /** @internal */
  id(): string { return this._id; }


  /** @internal */
  ngOnDestroy() {
    this._markerLayerManager.deleteMarker(this);
  }
}

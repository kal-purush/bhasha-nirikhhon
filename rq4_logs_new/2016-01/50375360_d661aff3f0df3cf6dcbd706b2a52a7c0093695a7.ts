
// NO  @scroll_container:  ()    -> $('#scrolling-container')
// NO  @set_display_parms: ()    => // Browser, etc:  @pixWindow
// NO  @scroll_to_ux_time: (uxt) ->
// NO  @scroll_to_thi:     ()    ->
// NO  @ux_time_of_pix:    (x)   ->
// NO  @scroll_to_tlo:     ()    => # bound
// NO  @set_time_cursor:   ()    => # bound
// NO  @scroll_monitor:    ()    => 

interface Meta {
  t1: number,
  t2: number,
  visible_time?: number
}

interface RequestData {
  meta: Meta
}


export class TimeVsPix {

  public base_time          = 0;        // aka @baseTime
  public window_width_secs  = 3 * 3600; // aka @timeWindow
  public window_width_pix   = 750;      // aka @pixWindow

  // DOM has data for time range [dom_time_lo..dom_time_hi] (at least)
  public dom_time_lo = null;            // aka @tlo
  public dom_time_hi = null;            // aka @thi

  // Returned from most recent data request (superfluous)
  public meta = {};                     // aka @meta
             
  constructor() {};

  merge_metadata(request_data: RequestData) {
    var meta = request_data.meta
    this.meta = meta
    if ( !this.base_time ) {
      this.base_time = meta['min_time']
    }
    var tlo = this.dom_time_lo;
    this.dom_time_lo = tlo ? Math.min(tlo, meta.t1) : meta.t1;
    var thi = this.dom_time_hi;
    this.dom_time_hi = thi ? Math.max(thi, meta.t2) : meta.t2;

    if (meta.visible_time) {
      this.window_width_secs = meta.visible_time
    }
  }

  nextHi() { return this.dom_time_hi + this.window_width_secs }

  nextLo() { return this.dom_time_lo - this.window_width_secs }

  // Ignoring base_time offset
  secsToPixScale(seconds) { return seconds * this.pixPerSec() }

  pixPerSec() { return this.window_width_pix / this.window_width_secs }

  pixToSecs(pix) { return this.base_time + Math.round(pix / this.pixPerSec()) }

  styleGeoHash(block) {
    var s: number, e: number;
    [s, e] = [block.starttime, block.endtime]          // per margins V
    return {
      left: `${this.secsToPixScale(s - this.base_time)}px`,
      width: `${this.secsToPixScale(e-s) - 4}px`
    }
  }
   
  uxTimeNow() { return new Date().valueOf() / 1000 }
   
  uxTimeOffset(ux_time) { return ux_time - this.base_time }

  uxTimeOffsetPix(ux_time) {
    return this.secsToPixScale( this.uxTimeOffset(ux_time) )
  }
  
  // Beyond here, scrolling, monitor, hairline time_cursor
  // in time_pix.js.coffee
}
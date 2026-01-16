const getScriptSrc = (callbackName) => {
  return `https://maps.googleapis.com/maps/api/js?key=AIzaSyAqg1BzcJOAGeLAIbA5tgrHBWtrfc6Rx5U&callback=${callbackName}`;
}

export class GmapService {

  private map: google.maps.Map;
  private geocoder: google.maps.Geocoder;
  private scriptLoadingPromise: Promise<void>;

  constructor() {
        //Loading script
        this.loadScriptLoadingPromise();
        //Loading other components
        this.onReady().then(() => {
          this.geocoder = new google.maps.Geocoder();
        });
  }

  onReady(): Promise<void> {
    return this.scriptLoadingPromise;
  }

  initMap(mapHtmlElement: HTMLElement, options: google.maps.MapOptions): Promise<google.maps.Map> {
    return this.onReady().then(() => {
      return this.map = new google.maps.Map(mapHtmlElement, options);
    });
  }

  private loadScriptLoadingPromise() {
    const script = window.document.createElement('script');
    script.type = 'text/javascript';
    script.async = true;
    script.defer = true;
    const callbackName: string = 'initMap';
    script.src = getScriptSrc(callbackName);
    this.scriptLoadingPromise = new Promise<void>((resolve: Function, reject: Function) => {
      (<any>window)[callbackName] = () => { resolve(); };

      script.onerror = (error: Event) => { reject(error); };
    });
    window.document.body.appendChild(script);
  }

  
}
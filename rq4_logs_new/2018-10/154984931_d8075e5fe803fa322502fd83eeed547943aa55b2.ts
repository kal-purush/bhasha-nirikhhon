import { AjaxService } from '@services/AjaxService';
import { PhotoService } from '@services/PhotoService';
import { PhotoStore } from '@src/features/photos/PhotoStore';
import { Constants } from '@src/utils/constants';

export class App {
  public stores: Record<string, any> = {};

  private ajaxService: AjaxService;
  private photoService: PhotoService;

  constructor() {
  }

  public init(): void {
    this.registerServices();
    this.registerStores();
  }

  private registerServices(): void {
    this.ajaxService = new AjaxService();
    this.photoService = new PhotoService(this.ajaxService);
  }

  private registerStores(): void {
    this.stores[Constants.STORE_PHOTO] = new PhotoStore(this.photoService);
  }
}
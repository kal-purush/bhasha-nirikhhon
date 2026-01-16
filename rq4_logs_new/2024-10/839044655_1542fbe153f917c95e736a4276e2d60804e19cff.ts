import { Injectable, Renderer2, RendererFactory2 } from '@angular/core';
import { Observable, of } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class ConfirmDialogService {
  private renderer:Renderer2;

  constructor(private rendererFactory:RendererFactory2) {
    this.renderer = this.rendererFactory.createRenderer(null, null);

  }

  public openConfirmDialog(title:string, description:string):Promise<boolean>{
    return new Promise((resolve) => {
      const overlay = this.renderer.createElement('div');
      this.renderer.addClass(overlay,'fixed');
      this.renderer.addClass(overlay,'inset-0');
      this.renderer.addClass(overlay,'flex');
      this.renderer.addClass(overlay,'justify-center');
      this.renderer.addClass(overlay,'items-center');
      this.renderer.addClass(overlay,'bg-black');
      this.renderer.addClass(overlay,'bg-opacity-50');

      const containerDialog = this.renderer.createElement('div');
      this.renderer.addClass(containerDialog,'h-[40%]');
      this.renderer.addClass(containerDialog,'w-[50%]');
      this.renderer.addClass(containerDialog,'px-[5%]');
      this.renderer.addClass(containerDialog,'py-[3%]');
      this.renderer.addClass(containerDialog,'flex');
      this.renderer.addClass(containerDialog,'flex-col');
      this.renderer.addClass(containerDialog,'justify-between');
      this.renderer.addClass(containerDialog,'items-center');
      this.renderer.addClass(containerDialog,'text-center');
      this.renderer.addClass(containerDialog,'bg-white');
      this.renderer.addClass(containerDialog,'rounded-xl');
      this.renderer.addClass(containerDialog,'shadow-xl');

      const containerTitle = this.renderer.createElement('div');
      const titleDialog = this.renderer.createText(title);
      this.renderer.appendChild(containerTitle,titleDialog);
      this.renderer.addClass(containerTitle, 'text-lg');
      this.renderer.addClass(containerTitle, 'font-bold');

      const containerDescription = this.renderer.createElement('div');
      const descriptionDialog = this.renderer.createText(description);
      this.renderer.appendChild(containerDescription,descriptionDialog);
      this.renderer.addClass(containerDescription, 'text-base');

      const containerButtons = this.renderer.createElement('div');
      this.renderer.addClass(containerButtons,'w-full');
      this.renderer.addClass(containerButtons,'flex');
      this.renderer.addClass(containerButtons,'text-white');

      this.renderer.addClass(containerButtons,'justify-between');

      const buttonAccept = this.createButton('Aceptar','bg-blue-800',resolve,overlay,true);
      const buttonCancel = this.createButton('Cancelar','bg-red-800',resolve,overlay,false);


      this.renderer.appendChild(containerDialog,containerTitle);
      this.renderer.appendChild(containerDialog,containerDescription);
      this.renderer.appendChild(containerButtons,buttonAccept);
      this.renderer.appendChild(containerButtons,buttonCancel);
      this.renderer.appendChild(containerDialog,containerButtons);
      this.renderer.appendChild(overlay,containerDialog);
      this.renderer.appendChild(document.body,overlay);
    });
  }


  public createButton(text:string, color:string, resolve:(value: boolean) => void, overlay:HTMLElement, returnValue:boolean){
    const button = this.renderer.createElement('button');
    const textButton = this.renderer.createText(text);
    this.renderer.appendChild(button,textButton);
    this.renderer.addClass(button,'w-[40%]');
    this.renderer.addClass(button,'py-2');
    this.renderer.addClass(button,'rounded-md');
    this.renderer.addClass(button,color);
    this.renderer.addClass(button,'bg-opacity-50');
    this.renderer.addClass(button,'hover:bg-opacity-100');

    this.renderer.listen(button, 'click', () => {
      resolve(returnValue);
      this.renderer.removeChild(document.body, overlay);
    });
    return button;
  }
}
import { Injectable, Renderer2, RendererFactory2 } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class ResquestLoaderRenderService {
  private renderer: Renderer2;


  public constructor(private rendererFactory: RendererFactory2) {
    this.renderer = this.rendererFactory.createRenderer(null, null);
  }

  public initRequestLoader(title:string, description:string):void{
    const overlay = this.renderer.createElement('div');
    this.renderer.addClass(overlay, 'flex');
    this.renderer.addClass(overlay, 'justify-center');
    this.renderer.addClass(overlay, 'items-center');
    this.renderer.addClass(overlay, 'fixed');
    this.renderer.addClass(overlay, 'inset-0');
    this.renderer.addClass(overlay, 'bg-black');
    this.renderer.addClass(overlay, 'bg-opacity-55');
    this.renderer.addClass(overlay, 'z-50');

    const container = this.renderer.createElement('div');
    this.renderer.addClass(container, 'w-[25%]');
    this.renderer.addClass(container, 'h-[50%]');
    this.renderer.addClass(container, 'px-2');
    this.renderer.addClass(container, 'py-[3%]');
    this.renderer.addClass(container, 'flex');
    this.renderer.addClass(container, 'flex-col');
    this.renderer.addClass(container, 'items-center');
    this.renderer.addClass(container, 'justify-between');
    this.renderer.addClass(container, 'text-center');
    this.renderer.addClass(container, 'rounded-3xl');
    this.renderer.addClass(container, 'animate-fadeIn');
    this.renderer.addClass(container, 'bg-white');
    this.renderer.addClass(container, 'shadow-2xl');

    const containerSpinner = this.renderer.createElement('div');
    this.renderer.addClass(containerSpinner, 'w-[80px]');
    this.renderer.addClass(containerSpinner, 'h-[80px]');
    this.renderer.addClass(containerSpinner, 'relative');
    this.renderer.addClass(containerSpinner, 'flex');
    this.renderer.addClass(containerSpinner, 'justify-center');
    this.renderer.addClass(containerSpinner, 'items-center');

    const borderSpinner = this.renderer.createElement('div');
    this.renderer.addClass(borderSpinner, 'w-full');
    this.renderer.addClass(borderSpinner, 'h-full');
    this.renderer.addClass(borderSpinner, 'border-[3px]');
    this.renderer.addClass(borderSpinner, 'border-[#19d71c]');
    this.renderer.addClass(borderSpinner, 'border-opacity-50');
    this.renderer.addClass(borderSpinner, 'rounded-full');

    const tailSpinner = this.renderer.createElement('img');
    tailSpinner.src = 'radio_media/tail-spin.svg';
    this.renderer.addClass(tailSpinner, 'w-full');
    this.renderer.addClass(tailSpinner, 'h-full');
    this.renderer.addClass(tailSpinner, 'absolute');

    const titleAlert = this.renderer.createText(title);
    const divTitleAlert = this.renderer.createElement('div');
    this.renderer.addClass(divTitleAlert, 'text-lg');
    this.renderer.addClass(divTitleAlert, 'font-bold');
    this.renderer.appendChild(divTitleAlert,titleAlert);

    const descriptionAlert = this.renderer.createText(description);
    const divDescriptionAlert = this.renderer.createElement('div');
    this.renderer.addClass(divDescriptionAlert, 'text-sm');
    this.renderer.appendChild(divDescriptionAlert,descriptionAlert);

    const buttonSucces = this.renderer.createElement('button');
    const buttonText = this.renderer.createText('Ok');
    this.renderer.appendChild(buttonSucces, buttonText);
    this.renderer.addClass(buttonSucces, 'w-full');
    this.renderer.addClass(buttonSucces, 'py-1');
    this.renderer.addClass(buttonSucces, 'text-white');
    this.renderer.addClass(buttonSucces, 'rounded-md');
    this.renderer.addClass(buttonSucces, 'bg-[#19d71c]');
    this.renderer.addClass(buttonSucces, 'bg-opacity-50');
    this.renderer.addClass(buttonSucces, 'cursor-default');

    this.renderer.appendChild(containerSpinner, borderSpinner);
    this.renderer.appendChild(containerSpinner, tailSpinner);
    this.renderer.appendChild(container, containerSpinner);
    this.renderer.appendChild(container,divTitleAlert);
    this.renderer.appendChild(container,divDescriptionAlert);
    this.renderer.appendChild(container,buttonSucces);
    this.renderer.appendChild(overlay,container);
    this.renderer.appendChild(document.body,overlay);

    this.loaderRequestLoader(overlay, containerSpinner, tailSpinner, buttonSucces);
  }

  public loaderRequestLoader(overlay:HTMLElement, containerSpinner:HTMLElement, tailSpinner:HTMLElement, buttonSucces:HTMLElement) {
    const checkSVG = this.renderer.createElement('img');
    checkSVG.src = 'radio_media/check-svgrepo-com.svg';
    this.renderer.addClass(checkSVG, 'absolute');
    this.renderer.addClass(checkSVG, 'animate-fadeIn');

    setTimeout(() => {
      this.renderer.removeChild(containerSpinner, tailSpinner);
      this.renderer.appendChild(containerSpinner,checkSVG);
      this.renderer.addClass(buttonSucces, 'bg-opacity-100');
      this.renderer.addClass(buttonSucces, 'hover:text-black');
      this.renderer.removeClass(buttonSucces,'cursor-default');
      this.renderer.addClass(buttonSucces, 'cursor-pointer');

      this.renderer.listen(buttonSucces, 'click', () => {
        this.closeRequestLoader(overlay);
      });

      this.renderer.listen(overlay, 'click', () => {
        this.closeRequestLoader(overlay);
      });
    }, 3000);


  }

  public closeRequestLoader(overlay:HTMLElement){
    this.renderer.removeChild(document.body, overlay);
  }
}
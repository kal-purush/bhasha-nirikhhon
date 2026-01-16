import {Transition, TransitionOptions} from './ngbTransition';

const collapseClass = 'collapse';
const collapsingClass = 'collapsing';
const showClass = 'show';
const dataHeightFrom = 'data-height-from';
const dataHeightTo = 'data-height-to';

export const collapsingEffet = {
  classname: 'collapsing',

  beforeTransitionStart: (panelElement: HTMLElement, options: TransitionOptions) => {
    const classList = panelElement.classList;
    // console.warn('beforeAnimationStart', panelElement, effectAction, effectAction.element.className);

    let heightInit;
    let heightFrom;
    let heightTo;

    if (classList.contains(collapsingClass)) {
      // Animation is running, revert it !
      heightInit = panelElement.getBoundingClientRect().height + 'px';
      heightFrom = panelElement.getAttribute(dataHeightTo);
      heightTo = panelElement.getAttribute(dataHeightFrom);
    } else {
      // Animation not running
      const isShown = classList.contains(showClass);

      // Reset the classes, for example if animation has been cancelled before
      classList.add(collapseClass);
      classList.remove(collapsingClass);

      // First set and store the height required for the animation
      if (!isShown) {
        classList.add(showClass);
      }

      panelElement.style.height = '';
      const height = panelElement.getBoundingClientRect().height + 'px';

      if (isShown) {
        heightFrom = height;
        heightTo = '0px';
      } else {
        heightFrom = '0px';
        heightTo = height;
      }
      heightInit = heightFrom;
    }

    // set the height before the animation
    panelElement.style.height = heightInit;
    panelElement.setAttribute(dataHeightFrom, heightFrom);
    panelElement.setAttribute(dataHeightTo, heightTo);

    // Remove the collapse classes to let the nbgAnimationEngine works by itself
    classList.remove(collapseClass);
    classList.remove(collapsingClass);
    classList.remove(showClass);

  },

  afterTransitionStart: (panelElement: HTMLElement, options: TransitionOptions) => {
    const heightTo = panelElement.getAttribute(dataHeightTo);
    const height = panelElement.getBoundingClientRect().height + 'px';
    if (heightTo === height) {
      return false;  // feedback that the animation cannot be done
    } else {
      panelElement.style.height = panelElement.getAttribute(dataHeightTo);
    }
  },

  afterTransitionEnd: (panelElement: HTMLElement, options: TransitionOptions) => {
    const classList = panelElement.classList;

    // Prepare the calback parameters
    // const callbackParameters = {
    //   isOpen: panelElement.getAttribute(dataHeightTo) !== '0px'
    // };
    const callbackParameters = {
      isOpen: options.enableAnimation ? panelElement.getAttribute(dataHeightTo) !== '0px' :
                                        !classList.contains(showClass)
    };

    // Togle classes
    classList.remove(collapsingClass);
    classList.add(collapseClass);
    classList.toggle(showClass, callbackParameters.isOpen);

    // Clean html data
    panelElement.removeAttribute(dataHeightFrom);
    panelElement.removeAttribute(dataHeightTo);
    panelElement.style.height = '';

    return callbackParameters;
  }
};


export const showEffect = {
  classname: 'show'
};
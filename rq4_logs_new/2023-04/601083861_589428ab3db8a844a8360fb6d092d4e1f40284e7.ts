const eventTypes = ['focusin', 'focusout', 'focus', 'blur'];

export type DomEventsProxy = {
  suspend: () => void;
  resume: () => void;
};

const addListeners = (element: Window | Document, listener: (event: Event) => void) => {
  eventTypes.forEach((type) => element.addEventListener(type, listener, true));
};

export const createDomEventsProxy = (): DomEventsProxy => {
  let shouldStopEvents = false;

  const listener = (event: Event) => {
    if (!shouldStopEvents) {
      return;
    }

    console.info('Cere Games SDK: Stop host event:', event.type, event);

    event.stopImmediatePropagation();
  };

  addListeners(document, listener);
  addListeners(window, listener);

  return {
    suspend: () => {
      console.info('Cere Games SDK: Suspend host events');

      shouldStopEvents = true;
    },

    resume: () => {
      console.info('Cere Games SDK: Resume host events');

      shouldStopEvents = false;
    },
  };
};
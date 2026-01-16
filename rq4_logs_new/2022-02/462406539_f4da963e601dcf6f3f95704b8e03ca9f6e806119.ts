import "@google/data-layer-helper";
import { getDataLayer } from "../core";
import { addEventContext } from "../helpers/addEventContext";
import log from "../log";

type Model = {
  page: {
    type: string;
  };
};

type Data = {
  event: string;
  [key: string]: any;
};

export type AdapterFunc = (model: Model, message: Data) => void;

export type Adapter = {
  id: string; // A unique id that is used to know if the adapter has loaded e.g. `Tealium`
  adapterFunc: AdapterFunc;
  hasLoaded: () => boolean;
};

const sendEventToAdapter = (model: Model, message: Data, adapter: Adapter) => {
  const { adapterFunc, id } = adapter;
  log.debug(`Sending event to ${id}`);

  // We need to handle any exceptions here and just log them, if we don't then the
  // datalayer legit just shuts down and assumes it's in a weird state
  try {
    adapterFunc(model, message);
  } catch (e) {
    log.warn(e);
  }
};

const attachAdapters = (adapters: Array<Adapter>) => {
  log.debug(`Creating listener`);
  const dataLayer = getDataLayer();

  const listener = (model: Model, message: Data): void => {
    log.debug("Processing event {model, message}", model, message);

    // Add event context (parent components etc)
    const modifiedMessage = addEventContext(message);

    adapters.forEach((adapter) => {
      sendEventToAdapter(model, modifiedMessage, adapter);
    });
  };

  // Attach the helper to a global so we can use it else where (e.g. in tests)
  window.TNZ.Tracking.dataLayerHelper = new window.DataLayerHelper(dataLayer, { listener, listenToPast: true });
};

const adapterLoaded = (adapters: Array<Adapter>): Function => {
  let ids = adapters.map((a) => a.id);

  return (id): void => {
    // Exit early if we've already created the data layer helper
    if (window.TNZ.Tracking?.dataLayerHelper) return;

    log.debug(`${id} adapter has loaded`);
    // Remove the adapter from our list of unloaded adapters
    ids = ids.filter((i) => i !== id);

    // Check if all the events have loaded
    if (ids.length > 0) return;

    // If they have we can then setup the data layer helper
    attachAdapters(adapters);
  };
};

/**
 * This takes in an array of Adapter instances which after waiting for the adapters to load,
 * it will then send events through to the adapters.
 *
 * We've made the assumption that adapters will be waiting for third party services to load
 * and that we'll need to track them loading/not loading before we start the stream of events.
 */
export const registerAdapters = (adapters: Array<Adapter>) => {
  // Register our adapter loaded function
  window.TNZ.Tracking.adapterLoaded = adapterLoaded(adapters);

  // Check if the adapters have already loaded
  const hasLoaded = adapters.filter((a) => a.hasLoaded()).length > 0;

  if (hasLoaded) {
    log.debug("All adapters have already loaded");
    // If they have then we can start the data layer listener without waiting for the
    // adapter to call `adapterLoaded`. We set `adapterLoaded` to a no-op to prevent
    // errors from the adapter trying to call it
    attachAdapters(adapters);
    return;
  }

  // Register our adapters
  window.TNZ.Tracking.adaptersToLoad = adapters.map((a) => a.id);

  log.debug("Adapter connection setup, ready to load adapters");
};
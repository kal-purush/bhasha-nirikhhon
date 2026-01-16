/**
 * Imports
 */
    // Dependencies
        import {ServiceRouter} from '../../../core/classes/ServiceRouter.ts';

    // Lot handlers
        import {getLot} from './lot/index.ts';

    // Provider handlers
        import {
            getProvider,
            getProviderProducts,
            getProviders,
            registerProvider,
            registerProviderProduct
        } from './provider/index.ts';

    // Product handlers
        import {
            getProduct,
            getProductLots,
            registerProductLot
        } from './product/index.ts';

/**
 * Initializing router
 */
    export var router = new ServiceRouter();

/**
 * Setting up router
 */
    // Provider services
    router.addService('getProvider', getProvider);
    router.addService('getProviderProducts', getProviderProducts);
    router.addService('getProviders', getProviders);
    router.addService('registerProvider', registerProvider);
    router.addService('registerProviderProduct', registerProviderProduct);

    // Product services
    router.addService('getProduct', getProduct);
    router.addService('getProductLots', getProductLots);
    router.addService('registerProductLot', registerProductLot);

    // Lot services
    router.addService('getLot', getLot);
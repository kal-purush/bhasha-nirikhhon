import {ComponentClass} from './ComponentClass';
import {ComponentInfo} from '../metadata';

/**
 * Injection target
 * @param <T> Target type
 */
class InjectionTarget<T> {
    readonly class: ComponentClass<T>;
    readonly info: ComponentInfo;
    readonly instance: T;

    /**
     * Class constructor
     * @param componentClass    Component class
     * @param componentInfo     Component info
     * @param componentInstance Component instance
     */
    constructor(componentClass: ComponentClass<T>, componentInfo: ComponentInfo, componentInstance: T) {
        this.class = componentClass;
        this.info = componentInfo;
        this.instance = componentInstance;
    }

}

export {
    InjectionTarget
};
import {setIfInterface} from '../utils';
import {setAnnotations} from '../reflection';

export function Constant<Type>(name: string, value: Type): Constant {

    var wrapper = {};

    setAnnotations(wrapper, [new ConstantAnnotation<Type>({
        name: name,
        constant: value
    })], 'value');

    return wrapper;

}

export interface ConstantOptions {
    name: string;
    constant: any;
}

export class ConstantAnnotation<Type> {

    name = '';
    constant: Type = null;

    constructor(options: ConstantOptions) {
        setIfInterface(this, options);
    }

}

export interface Constant {


}
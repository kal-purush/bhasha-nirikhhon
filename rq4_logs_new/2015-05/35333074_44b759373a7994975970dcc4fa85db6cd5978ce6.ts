module Listen {
    /*----------------*
     *   Decorators   *
     *----------------*/

    function SenderDecorator(): ISenderDecorator {
        return <T extends Function>(target: Object, key: string, descriptor: TypedPropertyDescriptor<T>): TypedPropertyDescriptor<T> => {
            descriptor = { get: Sender(key, descriptor.value) };
            return descriptor;
        };
    }

    function Sender<T extends Function>(key: string, func: T): any {
        return function (): ISender {
            Object.defineProperty(this, key, { value: func.bind(this) });
            this[key].targets = this[key].targets || [];
            return this[key];
        }
    }

    function ReceiverDecorator(): IReceiverDecorator {
        return <T extends Function>(target: Object, key: string, descriptor: TypedPropertyDescriptor<T>): TypedPropertyDescriptor<T> => {
            Object.defineProperty(target, key, { get: Receiver(key, descriptor.value) });
            descriptor.value = target[key];
            return descriptor;
        };
    }

    function Receiver<T extends Function>(key: string, func: T): any {
        return function (): IReceiver {
            Object.defineProperty(this, key, { value: func.bind(this) });
            return this[key];
        }
    }

    export var sender: ISenderDecorator = SenderDecorator();
    export var receiver: IReceiverDecorator = ReceiverDecorator();

    /*----------------*
     * Implementation *
     *----------------*/

    /*----------------*
     *   Interfaces   *
     *----------------*/

    interface IMethodDecorator {
        (target: Object, key: string, descriptor: TypedPropertyDescriptor<any>): TypedPropertyDescriptor<any>;// TypedPropertyDescriptor<any>;
    }

    interface IReceiverDecorator extends IMethodDecorator { }

    interface ISenderDecorator extends IMethodDecorator { }

    interface IReceiver extends Function {
    }

    interface ISender extends IReceiver {
        targets: IConnection[];
    }

    interface IConnection {
        sender: ISender;
        reciever: IReceiver;
    }
}

var sender = Listen.sender;
var receiver = Listen.receiver;
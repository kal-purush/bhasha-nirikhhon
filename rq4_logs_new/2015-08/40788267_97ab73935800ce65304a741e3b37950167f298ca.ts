///<reference path="../Utils/Collections/Map.ts"/>
///<reference path="../Utils/Collections/List.ts"/>
///<reference path="../core/Action.ts"/>
///<reference path="../core/EventManager.ts"/>

interface GameObjectId {
    id: string;
    type: string;
}



/**
 *  STAT COUNTER
 */
enum StatCounterModOper { DELTA, SET }

interface IEffect extends GameObjectId {

    parent: IBoardObject;
    effect_type: string;

    register(): void;
    unregister(): void;
}

interface IStatEffect extends IEffect {

    mods: Array<IStatCounterMod>;
}

interface IStatCounterMod {
    parent: IStatEffect;
    counterName: string;
    operation: StatCounterModOper;
    value: number;
}

interface IStatCounter extends GameObjectId {

    owner: IBoardObject;
    counterName: string;
    baseVal: number; // baseValue
    val: number;     // currentValue (before!) mods apply
    
    mods: Array<IStatCounterMod>;
    computedVal: number; // val after mods applied - has to be computed again every time mods changed
}




interface IBoardObject extends GameObjectId {

    stats: Array<IStatCounter>;

    statEffects: Array<IStatEffect>;

    tokens: Array<IToken>;

    effects: Array<IEffect>;

}





//interface IEffect extends GameObjectId {
//
//    // effectType: string;
//
//    event_type: string; // onTokenAdd, onTokenRemove
//    
//    parent: GameObjectId;
//
//    toDesc(): Object;
//
//    //    action
//}


interface IToken extends GameObjectId {
    owner: IBoardObject;
    tokenType: string;
    parent: IEffect;
}







interface ICard extends GameObjectId {

}

///<reference path="../Utils/Collections/Map.ts"/>
///<reference path="../Utils/Collections/List.ts"/>
///<reference path="../core/actions/Action.ts"/>
///<reference path="../core/EventManager.ts"/>




interface IGameObjectId {
    id   : string;
    type : string;
}

interface IGameObject extends IGameObjectId {
    
}

interface IMap<T> {
    [key: string] : T
}


interface IBoardObject extends IGameObject {

    owner: Player;

    source: IBoardObject; 

    stats: IMap<IStatCounter>;

    activatedAbilities: IActivate_Ability[];

    passiveAbilities: IPassive_Ablitity[];
    
     

    //statEffects: Array<IStatEffect>;

    //tokens: Array<IToken>;

    //effects: Array<IEffect>;

}


interface IStatCounter extends IGameObjectId {
    
    name: string;

    parent: IBoardObject;
    
    baseValue: number;
    
    value: number; 
}


interface ICounter extends IGameObjectId {

    parent: IGameObject;


}


interface IActivate_Ability extends IGameObjectId {

    parent: IBoardObject;

    getAction(): IAction;
}

interface IPassive_Ablitity extends IGameObjectId {

    //activator
    event_type: string;

    parent: IBoardObject;

    getAction(): IAction;
}



///**
// *  STAT COUNTER
// */
//enum StatCounterModOper { DELTA, SET }

//interface IEffect extends GameObject {

//    parent: IBoardObject;
//    effect_type: string;

//    register(): void;
//    unregister(): void;
//}

//interface IStatEffect extends IEffect {

//    mods: Array<IStatCounterMod>;
//}

//interface IStatCounterMod {
//    parent: IStatEffect;
//    counterName: string;
//    operation: StatCounterModOper;
//    value: number;
//}

//interface IStatCounter extends GameObjectID {

//    owner: IBoardObject;
//    counterName: string;
//    baseVal: number; // baseValue
//    val: number;     // currentValue (before!) mods apply
    
//    mods: Array<IStatCounterMod>;
//    computedVal: number; // val after mods applied - has to be computed again every time mods changed
//}










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


//interface IToken extends GameObjectID {
//    owner: IBoardObject;
//    tokenType: string;
//    parent: IEffect;
//}







//interface ICard extends GameObjectID {

//}

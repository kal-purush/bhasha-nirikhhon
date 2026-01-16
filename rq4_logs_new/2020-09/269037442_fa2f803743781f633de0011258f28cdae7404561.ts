import {
    /* Protocol */
    KoconutEquatable,

    /* Tool */
    KoconutPrimitive,
} from "../../../module.internal"

export class KoconutBoolean extends KoconutPrimitive<boolean> implements Boolean {

    constructor(boolean : boolean | null = null) {
        super()
        this.data = boolean == null ? false : boolean
    }

    valueOf() : boolean {
        return this.data!
    }

    and() {



    }

}
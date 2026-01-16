import {
    assert,
    method,
    prop,
    PubKeyHash,
    PubKey,
    SmartContract,
    Sig,
    hash160,
    SigHash,
    Utils,
    hash256,
} from 'scrypt-ts'


export class Escrow extends SmartContract {
    @prop()
    readonly buyerAddr: PubKeyHash

    @prop()
    readonly sellerAddr: PubKeyHash

    @prop()
    readonly arbiter: PubKeyHash

    constructor(
        buyerAddr: PubKeyHash,
        sellerAddr: PubKeyHash,
        arbiter: PubKeyHash,
    ) {
        super(...arguments)
        this.buyerAddr = buyerAddr
        this.sellerAddr = sellerAddr
        this.arbiter = arbiter
        
    }

    @method(SigHash.ANYONECANPAY_SINGLE)
    public confirmDeposit(
        buyerSig: Sig,
        buyerPubKey: PubKey,
        arbiter: PubKey,
        arbiterSig: Sig
        
    ) {
        // Validate buyer sig.
        assert(
            hash160(buyerPubKey) === this.buyerAddr,
            'invalid public key for buyer'
        )
        assert(
            this.checkSig(buyerSig, buyerPubKey),
            'buyer signature check failed'
        )
        
        // Validate arbiter sig.
        assert(
            hash160(arbiter) === this.arbiter,
            'invalid public key for arbiter'
        )
        assert(
            this.checkSig(arbiterSig, arbiter),
            'arbiter    signature check failed'
        )


        // Ensure arbiter gets paid.
        const deposit = this.ctx.utxo.value
        const out = Utils.buildPublicKeyHashOutput(this.arbiter, deposit)
        assert(hash256(out) === this.ctx.hashOutputs, 'hashOutputs mismatch')
    }

    @method(SigHash.ANYONECANPAY_SINGLE)
    public confirmPayment(
        buyerSig: Sig,
        buyerPubKey: PubKey,
        sellerSig: Sig,
        sellerPubKey : PubKey
    ) 
    {
        // Validate buyer sig.
        assert(
            hash160(buyerPubKey) === this.buyerAddr,
            'invalid public key for buyer'
        )
        assert(
            this.checkSig(buyerSig, buyerPubKey),
            'buyer signature check failed'
        )
        
        // Validate arbiter sig.
        assert(
            hash160(sellerPubKey) === this.sellerAddr,
            'invalid public key for seller'
        )
        assert(
            this.checkSig(sellerSig, sellerPubKey),
            'seller signature check failed'
        )

    
        // Ensure seller gets paid.
        const amount = this.ctx.utxo.value
        const out = Utils.buildPublicKeyHashOutput(this.sellerAddr, amount)
        assert(hash256(out) === this.ctx.hashOutputs, 'hashOutputs mismatch')
    }

        @method()
        public refund(
        buyerSig: Sig,
        buyerPubKey: PubKey,
        arbiter: PubKey,
        arbiterSig: Sig

    ) {

        // Validate arbiter sig.
        assert(
            hash160(arbiter) === this.arbiter,
            'invalid public key for buyer'
        )
        assert(
            this.checkSig(arbiterSig, arbiter),
            'buyer signature check failed'
        )

        // Validate buyer sig.
        assert(
            hash160(buyerPubKey) === this.buyerAddr,
            'invalid public key for buyer'
        )
        assert(
            this.checkSig(buyerSig, buyerPubKey),
            'buyer signature check failed'
        )


        // Ensure buyer gets refund.
        const amount = this.ctx.utxo.value
        const out = Utils.buildPublicKeyHashOutput(this.buyerAddr, amount)
        assert(hash256(out) === this.ctx.hashOutputs, 'hashOutputs mismatch')
        
    }


}
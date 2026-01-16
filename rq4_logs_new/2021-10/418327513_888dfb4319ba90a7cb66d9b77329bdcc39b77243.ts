const TYPES = {
    ISigner: Symbol.for("ISigner"),
    IdentityAuthProvider: Symbol.for("IdentityAuthProvider"),
    // Repo
    IdentityUserRepo: Symbol.for("IdentityUserRepo"),
    TraderRepo: Symbol.for("TraderRepo"),
    CardRepo:Symbol.for("CardRepo"),
    CardOrderRepo: Symbol.for("CardOrderRepo"),
    // Use Case
    RegisterUseCase: Symbol.for("RegisterUseCase"),
    AuthUseCase: Symbol.for("AuthUseCase"),
    IdentityTokenUseCase: Symbol.for("IdentityTokenUseCase"),
    CreateTraderUseCase: Symbol.for("CreateTraderUseCase"),
    CreateCardUseCase: Symbol.for("CreateCardUseCase"),
    FetchTraderUseCase: Symbol.for("FetchTraderUseCase"),
    PlaceOrderUseCase: Symbol.for("PlaceOrderUseCase"),
    CheckCardIndexUseCase: Symbol.for("CheckCardIndexUseCase"),
    GetOrdersUseCase: Symbol.for("GetOrdersUseCase"),
    // Controller
    TokenController: Symbol.for("TokenController"),
    RegisterController: Symbol.for("RegisterController"),
    CreateTraderController: Symbol.for("CreateTraderController"),
    CreateCardController: Symbol.for("CreateCardController"),
    PlaceOrderController: Symbol.for("PlaceOrderController"),
    GetOrdersController: Symbol.for("GetOrdersController")
};

export { TYPES };
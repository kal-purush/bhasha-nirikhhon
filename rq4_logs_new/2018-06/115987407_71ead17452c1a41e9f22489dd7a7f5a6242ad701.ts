type Long = protobuf.Long;

/** Namespace game. */
declare namespace game {

    /** Properties of a LoginReq. */
    interface ILoginReq {

        /** LoginReq uId */
        uId?: (string|null);

        /** LoginReq nick */
        nick?: (string|null);

        /** LoginReq photo */
        photo?: (string|null);

        /** LoginReq psw */
        psw?: (string|null);
    }

    /** Represents a LoginReq. */
    class LoginReq implements ILoginReq {

        /**
         * Constructs a new LoginReq.
         * @param [properties] Properties to set
         */
        constructor(properties?: game.ILoginReq);

        /** LoginReq uId. */
        public uId: string;

        /** LoginReq nick. */
        public nick: string;

        /** LoginReq photo. */
        public photo: string;

        /** LoginReq psw. */
        public psw: string;

        /**
         * Creates a new LoginReq instance using the specified properties.
         * @param [properties] Properties to set
         * @returns LoginReq instance
         */
        public static create(properties?: game.ILoginReq): game.LoginReq;

        /**
         * Encodes the specified LoginReq message. Does not implicitly {@link game.LoginReq.verify|verify} messages.
         * @param message LoginReq message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encode(message: game.ILoginReq, writer?: protobuf.Writer): protobuf.Writer;

        /**
         * Encodes the specified LoginReq message, length delimited. Does not implicitly {@link game.LoginReq.verify|verify} messages.
         * @param message LoginReq message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encodeDelimited(message: game.ILoginReq, writer?: protobuf.Writer): protobuf.Writer;

        /**
         * Decodes a LoginReq message from the specified reader or buffer.
         * @param reader Reader or buffer to decode from
         * @param [length] Message length if known beforehand
         * @returns LoginReq
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {protobuf.util.ProtocolError} If required fields are missing
         */
        public static decode(reader: (protobuf.Reader|Uint8Array), length?: number): game.LoginReq;

        /**
         * Decodes a LoginReq message from the specified reader or buffer, length delimited.
         * @param reader Reader or buffer to decode from
         * @returns LoginReq
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {protobuf.util.ProtocolError} If required fields are missing
         */
        public static decodeDelimited(reader: (protobuf.Reader|Uint8Array)): game.LoginReq;

        /**
         * Verifies a LoginReq message.
         * @param message Plain object to verify
         * @returns `null` if valid, otherwise the reason why it is not
         */
        public static verify(message: { [k: string]: any }): (string|null);

        /**
         * Creates a LoginReq message from a plain object. Also converts values to their respective internal types.
         * @param object Plain object
         * @returns LoginReq
         */
        public static fromObject(object: { [k: string]: any }): game.LoginReq;

        /**
         * Creates a plain object from a LoginReq message. Also converts values to other types if specified.
         * @param message LoginReq
         * @param [options] Conversion options
         * @returns Plain object
         */
        public static toObject(message: game.LoginReq, options?: protobuf.IConversionOptions): { [k: string]: any };

        /**
         * Converts this LoginReq to JSON.
         * @returns JSON object
         */
        public toJSON(): { [k: string]: any };
    }

    /** Properties of a LoginResp. */
    interface ILoginResp {

        /** LoginResp res */
        res?: (number|null);
    }

    /** Represents a LoginResp. */
    class LoginResp implements ILoginResp {

        /**
         * Constructs a new LoginResp.
         * @param [properties] Properties to set
         */
        constructor(properties?: game.ILoginResp);

        /** LoginResp res. */
        public res: number;

        /**
         * Creates a new LoginResp instance using the specified properties.
         * @param [properties] Properties to set
         * @returns LoginResp instance
         */
        public static create(properties?: game.ILoginResp): game.LoginResp;

        /**
         * Encodes the specified LoginResp message. Does not implicitly {@link game.LoginResp.verify|verify} messages.
         * @param message LoginResp message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encode(message: game.ILoginResp, writer?: protobuf.Writer): protobuf.Writer;

        /**
         * Encodes the specified LoginResp message, length delimited. Does not implicitly {@link game.LoginResp.verify|verify} messages.
         * @param message LoginResp message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encodeDelimited(message: game.ILoginResp, writer?: protobuf.Writer): protobuf.Writer;

        /**
         * Decodes a LoginResp message from the specified reader or buffer.
         * @param reader Reader or buffer to decode from
         * @param [length] Message length if known beforehand
         * @returns LoginResp
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {protobuf.util.ProtocolError} If required fields are missing
         */
        public static decode(reader: (protobuf.Reader|Uint8Array), length?: number): game.LoginResp;

        /**
         * Decodes a LoginResp message from the specified reader or buffer, length delimited.
         * @param reader Reader or buffer to decode from
         * @returns LoginResp
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {protobuf.util.ProtocolError} If required fields are missing
         */
        public static decodeDelimited(reader: (protobuf.Reader|Uint8Array)): game.LoginResp;

        /**
         * Verifies a LoginResp message.
         * @param message Plain object to verify
         * @returns `null` if valid, otherwise the reason why it is not
         */
        public static verify(message: { [k: string]: any }): (string|null);

        /**
         * Creates a LoginResp message from a plain object. Also converts values to their respective internal types.
         * @param object Plain object
         * @returns LoginResp
         */
        public static fromObject(object: { [k: string]: any }): game.LoginResp;

        /**
         * Creates a plain object from a LoginResp message. Also converts values to other types if specified.
         * @param message LoginResp
         * @param [options] Conversion options
         * @returns Plain object
         */
        public static toObject(message: game.LoginResp, options?: protobuf.IConversionOptions): { [k: string]: any };

        /**
         * Converts this LoginResp to JSON.
         * @returns JSON object
         */
        public toJSON(): { [k: string]: any };
    }

    /** Properties of a CreateRoomReq. */
    interface ICreateRoomReq {

        /** CreateRoomReq nSumUser */
        nSumUser?: (number|null);

        /** CreateRoomReq skin */
        skin?: (number|null);
    }

    /** Represents a CreateRoomReq. */
    class CreateRoomReq implements ICreateRoomReq {

        /**
         * Constructs a new CreateRoomReq.
         * @param [properties] Properties to set
         */
        constructor(properties?: game.ICreateRoomReq);

        /** CreateRoomReq nSumUser. */
        public nSumUser: number;

        /** CreateRoomReq skin. */
        public skin: number;

        /**
         * Creates a new CreateRoomReq instance using the specified properties.
         * @param [properties] Properties to set
         * @returns CreateRoomReq instance
         */
        public static create(properties?: game.ICreateRoomReq): game.CreateRoomReq;

        /**
         * Encodes the specified CreateRoomReq message. Does not implicitly {@link game.CreateRoomReq.verify|verify} messages.
         * @param message CreateRoomReq message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encode(message: game.ICreateRoomReq, writer?: protobuf.Writer): protobuf.Writer;

        /**
         * Encodes the specified CreateRoomReq message, length delimited. Does not implicitly {@link game.CreateRoomReq.verify|verify} messages.
         * @param message CreateRoomReq message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encodeDelimited(message: game.ICreateRoomReq, writer?: protobuf.Writer): protobuf.Writer;

        /**
         * Decodes a CreateRoomReq message from the specified reader or buffer.
         * @param reader Reader or buffer to decode from
         * @param [length] Message length if known beforehand
         * @returns CreateRoomReq
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {protobuf.util.ProtocolError} If required fields are missing
         */
        public static decode(reader: (protobuf.Reader|Uint8Array), length?: number): game.CreateRoomReq;

        /**
         * Decodes a CreateRoomReq message from the specified reader or buffer, length delimited.
         * @param reader Reader or buffer to decode from
         * @returns CreateRoomReq
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {protobuf.util.ProtocolError} If required fields are missing
         */
        public static decodeDelimited(reader: (protobuf.Reader|Uint8Array)): game.CreateRoomReq;

        /**
         * Verifies a CreateRoomReq message.
         * @param message Plain object to verify
         * @returns `null` if valid, otherwise the reason why it is not
         */
        public static verify(message: { [k: string]: any }): (string|null);

        /**
         * Creates a CreateRoomReq message from a plain object. Also converts values to their respective internal types.
         * @param object Plain object
         * @returns CreateRoomReq
         */
        public static fromObject(object: { [k: string]: any }): game.CreateRoomReq;

        /**
         * Creates a plain object from a CreateRoomReq message. Also converts values to other types if specified.
         * @param message CreateRoomReq
         * @param [options] Conversion options
         * @returns Plain object
         */
        public static toObject(message: game.CreateRoomReq, options?: protobuf.IConversionOptions): { [k: string]: any };

        /**
         * Converts this CreateRoomReq to JSON.
         * @returns JSON object
         */
        public toJSON(): { [k: string]: any };
    }

    /** Properties of a CreateRoomResp. */
    interface ICreateRoomResp {

        /** CreateRoomResp res */
        res?: (number|null);

        /** CreateRoomResp nIdRoom */
        nIdRoom?: (number|null);

        /** CreateRoomResp nSumUser */
        nSumUser?: (number|null);
    }

    /** Represents a CreateRoomResp. */
    class CreateRoomResp implements ICreateRoomResp {

        /**
         * Constructs a new CreateRoomResp.
         * @param [properties] Properties to set
         */
        constructor(properties?: game.ICreateRoomResp);

        /** CreateRoomResp res. */
        public res: number;

        /** CreateRoomResp nIdRoom. */
        public nIdRoom: number;

        /** CreateRoomResp nSumUser. */
        public nSumUser: number;

        /**
         * Creates a new CreateRoomResp instance using the specified properties.
         * @param [properties] Properties to set
         * @returns CreateRoomResp instance
         */
        public static create(properties?: game.ICreateRoomResp): game.CreateRoomResp;

        /**
         * Encodes the specified CreateRoomResp message. Does not implicitly {@link game.CreateRoomResp.verify|verify} messages.
         * @param message CreateRoomResp message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encode(message: game.ICreateRoomResp, writer?: protobuf.Writer): protobuf.Writer;

        /**
         * Encodes the specified CreateRoomResp message, length delimited. Does not implicitly {@link game.CreateRoomResp.verify|verify} messages.
         * @param message CreateRoomResp message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encodeDelimited(message: game.ICreateRoomResp, writer?: protobuf.Writer): protobuf.Writer;

        /**
         * Decodes a CreateRoomResp message from the specified reader or buffer.
         * @param reader Reader or buffer to decode from
         * @param [length] Message length if known beforehand
         * @returns CreateRoomResp
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {protobuf.util.ProtocolError} If required fields are missing
         */
        public static decode(reader: (protobuf.Reader|Uint8Array), length?: number): game.CreateRoomResp;

        /**
         * Decodes a CreateRoomResp message from the specified reader or buffer, length delimited.
         * @param reader Reader or buffer to decode from
         * @returns CreateRoomResp
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {protobuf.util.ProtocolError} If required fields are missing
         */
        public static decodeDelimited(reader: (protobuf.Reader|Uint8Array)): game.CreateRoomResp;

        /**
         * Verifies a CreateRoomResp message.
         * @param message Plain object to verify
         * @returns `null` if valid, otherwise the reason why it is not
         */
        public static verify(message: { [k: string]: any }): (string|null);

        /**
         * Creates a CreateRoomResp message from a plain object. Also converts values to their respective internal types.
         * @param object Plain object
         * @returns CreateRoomResp
         */
        public static fromObject(object: { [k: string]: any }): game.CreateRoomResp;

        /**
         * Creates a plain object from a CreateRoomResp message. Also converts values to other types if specified.
         * @param message CreateRoomResp
         * @param [options] Conversion options
         * @returns Plain object
         */
        public static toObject(message: game.CreateRoomResp, options?: protobuf.IConversionOptions): { [k: string]: any };

        /**
         * Converts this CreateRoomResp to JSON.
         * @returns JSON object
         */
        public toJSON(): { [k: string]: any };
    }

    /** Properties of a JoinRoomReq. */
    interface IJoinRoomReq {

        /** JoinRoomReq nIdRoom */
        nIdRoom?: (number|null);
    }

    /** Represents a JoinRoomReq. */
    class JoinRoomReq implements IJoinRoomReq {

        /**
         * Constructs a new JoinRoomReq.
         * @param [properties] Properties to set
         */
        constructor(properties?: game.IJoinRoomReq);

        /** JoinRoomReq nIdRoom. */
        public nIdRoom: number;

        /**
         * Creates a new JoinRoomReq instance using the specified properties.
         * @param [properties] Properties to set
         * @returns JoinRoomReq instance
         */
        public static create(properties?: game.IJoinRoomReq): game.JoinRoomReq;

        /**
         * Encodes the specified JoinRoomReq message. Does not implicitly {@link game.JoinRoomReq.verify|verify} messages.
         * @param message JoinRoomReq message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encode(message: game.IJoinRoomReq, writer?: protobuf.Writer): protobuf.Writer;

        /**
         * Encodes the specified JoinRoomReq message, length delimited. Does not implicitly {@link game.JoinRoomReq.verify|verify} messages.
         * @param message JoinRoomReq message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encodeDelimited(message: game.IJoinRoomReq, writer?: protobuf.Writer): protobuf.Writer;

        /**
         * Decodes a JoinRoomReq message from the specified reader or buffer.
         * @param reader Reader or buffer to decode from
         * @param [length] Message length if known beforehand
         * @returns JoinRoomReq
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {protobuf.util.ProtocolError} If required fields are missing
         */
        public static decode(reader: (protobuf.Reader|Uint8Array), length?: number): game.JoinRoomReq;

        /**
         * Decodes a JoinRoomReq message from the specified reader or buffer, length delimited.
         * @param reader Reader or buffer to decode from
         * @returns JoinRoomReq
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {protobuf.util.ProtocolError} If required fields are missing
         */
        public static decodeDelimited(reader: (protobuf.Reader|Uint8Array)): game.JoinRoomReq;

        /**
         * Verifies a JoinRoomReq message.
         * @param message Plain object to verify
         * @returns `null` if valid, otherwise the reason why it is not
         */
        public static verify(message: { [k: string]: any }): (string|null);

        /**
         * Creates a JoinRoomReq message from a plain object. Also converts values to their respective internal types.
         * @param object Plain object
         * @returns JoinRoomReq
         */
        public static fromObject(object: { [k: string]: any }): game.JoinRoomReq;

        /**
         * Creates a plain object from a JoinRoomReq message. Also converts values to other types if specified.
         * @param message JoinRoomReq
         * @param [options] Conversion options
         * @returns Plain object
         */
        public static toObject(message: game.JoinRoomReq, options?: protobuf.IConversionOptions): { [k: string]: any };

        /**
         * Converts this JoinRoomReq to JSON.
         * @returns JSON object
         */
        public toJSON(): { [k: string]: any };
    }

    /** Properties of a JoinRoomResp. */
    interface IJoinRoomResp {

        /** JoinRoomResp res */
        res?: (number|null);

        /** JoinRoomResp nIdRoom */
        nIdRoom?: (number|null);

        /** JoinRoomResp nSumUser */
        nSumUser?: (number|null);

        /** JoinRoomResp uIdHost */
        uIdHost?: (string|null);

        /** JoinRoomResp users */
        users?: (game.IUserJoin[]|null);
    }

    /** Represents a JoinRoomResp. */
    class JoinRoomResp implements IJoinRoomResp {

        /**
         * Constructs a new JoinRoomResp.
         * @param [properties] Properties to set
         */
        constructor(properties?: game.IJoinRoomResp);

        /** JoinRoomResp res. */
        public res: number;

        /** JoinRoomResp nIdRoom. */
        public nIdRoom: number;

        /** JoinRoomResp nSumUser. */
        public nSumUser: number;

        /** JoinRoomResp uIdHost. */
        public uIdHost: string;

        /** JoinRoomResp users. */
        public users: game.IUserJoin[];

        /**
         * Creates a new JoinRoomResp instance using the specified properties.
         * @param [properties] Properties to set
         * @returns JoinRoomResp instance
         */
        public static create(properties?: game.IJoinRoomResp): game.JoinRoomResp;

        /**
         * Encodes the specified JoinRoomResp message. Does not implicitly {@link game.JoinRoomResp.verify|verify} messages.
         * @param message JoinRoomResp message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encode(message: game.IJoinRoomResp, writer?: protobuf.Writer): protobuf.Writer;

        /**
         * Encodes the specified JoinRoomResp message, length delimited. Does not implicitly {@link game.JoinRoomResp.verify|verify} messages.
         * @param message JoinRoomResp message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encodeDelimited(message: game.IJoinRoomResp, writer?: protobuf.Writer): protobuf.Writer;

        /**
         * Decodes a JoinRoomResp message from the specified reader or buffer.
         * @param reader Reader or buffer to decode from
         * @param [length] Message length if known beforehand
         * @returns JoinRoomResp
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {protobuf.util.ProtocolError} If required fields are missing
         */
        public static decode(reader: (protobuf.Reader|Uint8Array), length?: number): game.JoinRoomResp;

        /**
         * Decodes a JoinRoomResp message from the specified reader or buffer, length delimited.
         * @param reader Reader or buffer to decode from
         * @returns JoinRoomResp
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {protobuf.util.ProtocolError} If required fields are missing
         */
        public static decodeDelimited(reader: (protobuf.Reader|Uint8Array)): game.JoinRoomResp;

        /**
         * Verifies a JoinRoomResp message.
         * @param message Plain object to verify
         * @returns `null` if valid, otherwise the reason why it is not
         */
        public static verify(message: { [k: string]: any }): (string|null);

        /**
         * Creates a JoinRoomResp message from a plain object. Also converts values to their respective internal types.
         * @param object Plain object
         * @returns JoinRoomResp
         */
        public static fromObject(object: { [k: string]: any }): game.JoinRoomResp;

        /**
         * Creates a plain object from a JoinRoomResp message. Also converts values to other types if specified.
         * @param message JoinRoomResp
         * @param [options] Conversion options
         * @returns Plain object
         */
        public static toObject(message: game.JoinRoomResp, options?: protobuf.IConversionOptions): { [k: string]: any };

        /**
         * Converts this JoinRoomResp to JSON.
         * @returns JSON object
         */
        public toJSON(): { [k: string]: any };
    }

    /** Properties of a UserJoin. */
    interface IUserJoin {

        /** UserJoin uId */
        uId?: (string|null);

        /** UserJoin nick */
        nick?: (string|null);

        /** UserJoin photo */
        photo?: (string|null);

        /** UserJoin nPos */
        nPos?: (number|null);

        /** UserJoin skin */
        skin?: (number|null);
    }

    /** Represents a UserJoin. */
    class UserJoin implements IUserJoin {

        /**
         * Constructs a new UserJoin.
         * @param [properties] Properties to set
         */
        constructor(properties?: game.IUserJoin);

        /** UserJoin uId. */
        public uId: string;

        /** UserJoin nick. */
        public nick: string;

        /** UserJoin photo. */
        public photo: string;

        /** UserJoin nPos. */
        public nPos: number;

        /** UserJoin skin. */
        public skin: number;

        /**
         * Creates a new UserJoin instance using the specified properties.
         * @param [properties] Properties to set
         * @returns UserJoin instance
         */
        public static create(properties?: game.IUserJoin): game.UserJoin;

        /**
         * Encodes the specified UserJoin message. Does not implicitly {@link game.UserJoin.verify|verify} messages.
         * @param message UserJoin message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encode(message: game.IUserJoin, writer?: protobuf.Writer): protobuf.Writer;

        /**
         * Encodes the specified UserJoin message, length delimited. Does not implicitly {@link game.UserJoin.verify|verify} messages.
         * @param message UserJoin message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encodeDelimited(message: game.IUserJoin, writer?: protobuf.Writer): protobuf.Writer;

        /**
         * Decodes a UserJoin message from the specified reader or buffer.
         * @param reader Reader or buffer to decode from
         * @param [length] Message length if known beforehand
         * @returns UserJoin
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {protobuf.util.ProtocolError} If required fields are missing
         */
        public static decode(reader: (protobuf.Reader|Uint8Array), length?: number): game.UserJoin;

        /**
         * Decodes a UserJoin message from the specified reader or buffer, length delimited.
         * @param reader Reader or buffer to decode from
         * @returns UserJoin
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {protobuf.util.ProtocolError} If required fields are missing
         */
        public static decodeDelimited(reader: (protobuf.Reader|Uint8Array)): game.UserJoin;

        /**
         * Verifies a UserJoin message.
         * @param message Plain object to verify
         * @returns `null` if valid, otherwise the reason why it is not
         */
        public static verify(message: { [k: string]: any }): (string|null);

        /**
         * Creates a UserJoin message from a plain object. Also converts values to their respective internal types.
         * @param object Plain object
         * @returns UserJoin
         */
        public static fromObject(object: { [k: string]: any }): game.UserJoin;

        /**
         * Creates a plain object from a UserJoin message. Also converts values to other types if specified.
         * @param message UserJoin
         * @param [options] Conversion options
         * @returns Plain object
         */
        public static toObject(message: game.UserJoin, options?: protobuf.IConversionOptions): { [k: string]: any };

        /**
         * Converts this UserJoin to JSON.
         * @returns JSON object
         */
        public toJSON(): { [k: string]: any };
    }

    /** Properties of a StartGameReq. */
    interface IStartGameReq {

        /** StartGameReq n */
        n?: (string|null);
    }

    /** Represents a StartGameReq. */
    class StartGameReq implements IStartGameReq {

        /**
         * Constructs a new StartGameReq.
         * @param [properties] Properties to set
         */
        constructor(properties?: game.IStartGameReq);

        /** StartGameReq n. */
        public n: string;

        /**
         * Creates a new StartGameReq instance using the specified properties.
         * @param [properties] Properties to set
         * @returns StartGameReq instance
         */
        public static create(properties?: game.IStartGameReq): game.StartGameReq;

        /**
         * Encodes the specified StartGameReq message. Does not implicitly {@link game.StartGameReq.verify|verify} messages.
         * @param message StartGameReq message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encode(message: game.IStartGameReq, writer?: protobuf.Writer): protobuf.Writer;

        /**
         * Encodes the specified StartGameReq message, length delimited. Does not implicitly {@link game.StartGameReq.verify|verify} messages.
         * @param message StartGameReq message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encodeDelimited(message: game.IStartGameReq, writer?: protobuf.Writer): protobuf.Writer;

        /**
         * Decodes a StartGameReq message from the specified reader or buffer.
         * @param reader Reader or buffer to decode from
         * @param [length] Message length if known beforehand
         * @returns StartGameReq
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {protobuf.util.ProtocolError} If required fields are missing
         */
        public static decode(reader: (protobuf.Reader|Uint8Array), length?: number): game.StartGameReq;

        /**
         * Decodes a StartGameReq message from the specified reader or buffer, length delimited.
         * @param reader Reader or buffer to decode from
         * @returns StartGameReq
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {protobuf.util.ProtocolError} If required fields are missing
         */
        public static decodeDelimited(reader: (protobuf.Reader|Uint8Array)): game.StartGameReq;

        /**
         * Verifies a StartGameReq message.
         * @param message Plain object to verify
         * @returns `null` if valid, otherwise the reason why it is not
         */
        public static verify(message: { [k: string]: any }): (string|null);

        /**
         * Creates a StartGameReq message from a plain object. Also converts values to their respective internal types.
         * @param object Plain object
         * @returns StartGameReq
         */
        public static fromObject(object: { [k: string]: any }): game.StartGameReq;

        /**
         * Creates a plain object from a StartGameReq message. Also converts values to other types if specified.
         * @param message StartGameReq
         * @param [options] Conversion options
         * @returns Plain object
         */
        public static toObject(message: game.StartGameReq, options?: protobuf.IConversionOptions): { [k: string]: any };

        /**
         * Converts this StartGameReq to JSON.
         * @returns JSON object
         */
        public toJSON(): { [k: string]: any };
    }

    /** Properties of a StartGameResp. */
    interface IStartGameResp {

        /** StartGameResp res */
        res?: (number|null);
    }

    /** Represents a StartGameResp. */
    class StartGameResp implements IStartGameResp {

        /**
         * Constructs a new StartGameResp.
         * @param [properties] Properties to set
         */
        constructor(properties?: game.IStartGameResp);

        /** StartGameResp res. */
        public res: number;

        /**
         * Creates a new StartGameResp instance using the specified properties.
         * @param [properties] Properties to set
         * @returns StartGameResp instance
         */
        public static create(properties?: game.IStartGameResp): game.StartGameResp;

        /**
         * Encodes the specified StartGameResp message. Does not implicitly {@link game.StartGameResp.verify|verify} messages.
         * @param message StartGameResp message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encode(message: game.IStartGameResp, writer?: protobuf.Writer): protobuf.Writer;

        /**
         * Encodes the specified StartGameResp message, length delimited. Does not implicitly {@link game.StartGameResp.verify|verify} messages.
         * @param message StartGameResp message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encodeDelimited(message: game.IStartGameResp, writer?: protobuf.Writer): protobuf.Writer;

        /**
         * Decodes a StartGameResp message from the specified reader or buffer.
         * @param reader Reader or buffer to decode from
         * @param [length] Message length if known beforehand
         * @returns StartGameResp
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {protobuf.util.ProtocolError} If required fields are missing
         */
        public static decode(reader: (protobuf.Reader|Uint8Array), length?: number): game.StartGameResp;

        /**
         * Decodes a StartGameResp message from the specified reader or buffer, length delimited.
         * @param reader Reader or buffer to decode from
         * @returns StartGameResp
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {protobuf.util.ProtocolError} If required fields are missing
         */
        public static decodeDelimited(reader: (protobuf.Reader|Uint8Array)): game.StartGameResp;

        /**
         * Verifies a StartGameResp message.
         * @param message Plain object to verify
         * @returns `null` if valid, otherwise the reason why it is not
         */
        public static verify(message: { [k: string]: any }): (string|null);

        /**
         * Creates a StartGameResp message from a plain object. Also converts values to their respective internal types.
         * @param object Plain object
         * @returns StartGameResp
         */
        public static fromObject(object: { [k: string]: any }): game.StartGameResp;

        /**
         * Creates a plain object from a StartGameResp message. Also converts values to other types if specified.
         * @param message StartGameResp
         * @param [options] Conversion options
         * @returns Plain object
         */
        public static toObject(message: game.StartGameResp, options?: protobuf.IConversionOptions): { [k: string]: any };

        /**
         * Converts this StartGameResp to JSON.
         * @returns JSON object
         */
        public toJSON(): { [k: string]: any };
    }

    /** Properties of an OperateLuckyNextResp. */
    interface IOperateLuckyNextResp {

        /** OperateLuckyNextResp nPos */
        nPos?: (number|null);
    }

    /** Represents an OperateLuckyNextResp. */
    class OperateLuckyNextResp implements IOperateLuckyNextResp {

        /**
         * Constructs a new OperateLuckyNextResp.
         * @param [properties] Properties to set
         */
        constructor(properties?: game.IOperateLuckyNextResp);

        /** OperateLuckyNextResp nPos. */
        public nPos: number;

        /**
         * Creates a new OperateLuckyNextResp instance using the specified properties.
         * @param [properties] Properties to set
         * @returns OperateLuckyNextResp instance
         */
        public static create(properties?: game.IOperateLuckyNextResp): game.OperateLuckyNextResp;

        /**
         * Encodes the specified OperateLuckyNextResp message. Does not implicitly {@link game.OperateLuckyNextResp.verify|verify} messages.
         * @param message OperateLuckyNextResp message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encode(message: game.IOperateLuckyNextResp, writer?: protobuf.Writer): protobuf.Writer;

        /**
         * Encodes the specified OperateLuckyNextResp message, length delimited. Does not implicitly {@link game.OperateLuckyNextResp.verify|verify} messages.
         * @param message OperateLuckyNextResp message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encodeDelimited(message: game.IOperateLuckyNextResp, writer?: protobuf.Writer): protobuf.Writer;

        /**
         * Decodes an OperateLuckyNextResp message from the specified reader or buffer.
         * @param reader Reader or buffer to decode from
         * @param [length] Message length if known beforehand
         * @returns OperateLuckyNextResp
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {protobuf.util.ProtocolError} If required fields are missing
         */
        public static decode(reader: (protobuf.Reader|Uint8Array), length?: number): game.OperateLuckyNextResp;

        /**
         * Decodes an OperateLuckyNextResp message from the specified reader or buffer, length delimited.
         * @param reader Reader or buffer to decode from
         * @returns OperateLuckyNextResp
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {protobuf.util.ProtocolError} If required fields are missing
         */
        public static decodeDelimited(reader: (protobuf.Reader|Uint8Array)): game.OperateLuckyNextResp;

        /**
         * Verifies an OperateLuckyNextResp message.
         * @param message Plain object to verify
         * @returns `null` if valid, otherwise the reason why it is not
         */
        public static verify(message: { [k: string]: any }): (string|null);

        /**
         * Creates an OperateLuckyNextResp message from a plain object. Also converts values to their respective internal types.
         * @param object Plain object
         * @returns OperateLuckyNextResp
         */
        public static fromObject(object: { [k: string]: any }): game.OperateLuckyNextResp;

        /**
         * Creates a plain object from an OperateLuckyNextResp message. Also converts values to other types if specified.
         * @param message OperateLuckyNextResp
         * @param [options] Conversion options
         * @returns Plain object
         */
        public static toObject(message: game.OperateLuckyNextResp, options?: protobuf.IConversionOptions): { [k: string]: any };

        /**
         * Converts this OperateLuckyNextResp to JSON.
         * @returns JSON object
         */
        public toJSON(): { [k: string]: any };
    }

    /** Properties of an OperateLuckyEndReq. */
    interface IOperateLuckyEndReq {

        /** OperateLuckyEndReq nPos */
        nPos?: (number|null);
    }

    /** Represents an OperateLuckyEndReq. */
    class OperateLuckyEndReq implements IOperateLuckyEndReq {

        /**
         * Constructs a new OperateLuckyEndReq.
         * @param [properties] Properties to set
         */
        constructor(properties?: game.IOperateLuckyEndReq);

        /** OperateLuckyEndReq nPos. */
        public nPos: number;

        /**
         * Creates a new OperateLuckyEndReq instance using the specified properties.
         * @param [properties] Properties to set
         * @returns OperateLuckyEndReq instance
         */
        public static create(properties?: game.IOperateLuckyEndReq): game.OperateLuckyEndReq;

        /**
         * Encodes the specified OperateLuckyEndReq message. Does not implicitly {@link game.OperateLuckyEndReq.verify|verify} messages.
         * @param message OperateLuckyEndReq message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encode(message: game.IOperateLuckyEndReq, writer?: protobuf.Writer): protobuf.Writer;

        /**
         * Encodes the specified OperateLuckyEndReq message, length delimited. Does not implicitly {@link game.OperateLuckyEndReq.verify|verify} messages.
         * @param message OperateLuckyEndReq message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encodeDelimited(message: game.IOperateLuckyEndReq, writer?: protobuf.Writer): protobuf.Writer;

        /**
         * Decodes an OperateLuckyEndReq message from the specified reader or buffer.
         * @param reader Reader or buffer to decode from
         * @param [length] Message length if known beforehand
         * @returns OperateLuckyEndReq
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {protobuf.util.ProtocolError} If required fields are missing
         */
        public static decode(reader: (protobuf.Reader|Uint8Array), length?: number): game.OperateLuckyEndReq;

        /**
         * Decodes an OperateLuckyEndReq message from the specified reader or buffer, length delimited.
         * @param reader Reader or buffer to decode from
         * @returns OperateLuckyEndReq
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {protobuf.util.ProtocolError} If required fields are missing
         */
        public static decodeDelimited(reader: (protobuf.Reader|Uint8Array)): game.OperateLuckyEndReq;

        /**
         * Verifies an OperateLuckyEndReq message.
         * @param message Plain object to verify
         * @returns `null` if valid, otherwise the reason why it is not
         */
        public static verify(message: { [k: string]: any }): (string|null);

        /**
         * Creates an OperateLuckyEndReq message from a plain object. Also converts values to their respective internal types.
         * @param object Plain object
         * @returns OperateLuckyEndReq
         */
        public static fromObject(object: { [k: string]: any }): game.OperateLuckyEndReq;

        /**
         * Creates a plain object from an OperateLuckyEndReq message. Also converts values to other types if specified.
         * @param message OperateLuckyEndReq
         * @param [options] Conversion options
         * @returns Plain object
         */
        public static toObject(message: game.OperateLuckyEndReq, options?: protobuf.IConversionOptions): { [k: string]: any };

        /**
         * Converts this OperateLuckyEndReq to JSON.
         * @returns JSON object
         */
        public toJSON(): { [k: string]: any };
    }

    /** Properties of an OperateLuckyEndResp. */
    interface IOperateLuckyEndResp {

        /** OperateLuckyEndResp res */
        res?: (number|null);

        /** OperateLuckyEndResp nPos */
        nPos?: (number|null);

        /** OperateLuckyEndResp n */
        n?: (number|null);
    }

    /** Represents an OperateLuckyEndResp. */
    class OperateLuckyEndResp implements IOperateLuckyEndResp {

        /**
         * Constructs a new OperateLuckyEndResp.
         * @param [properties] Properties to set
         */
        constructor(properties?: game.IOperateLuckyEndResp);

        /** OperateLuckyEndResp res. */
        public res: number;

        /** OperateLuckyEndResp nPos. */
        public nPos: number;

        /** OperateLuckyEndResp n. */
        public n: number;

        /**
         * Creates a new OperateLuckyEndResp instance using the specified properties.
         * @param [properties] Properties to set
         * @returns OperateLuckyEndResp instance
         */
        public static create(properties?: game.IOperateLuckyEndResp): game.OperateLuckyEndResp;

        /**
         * Encodes the specified OperateLuckyEndResp message. Does not implicitly {@link game.OperateLuckyEndResp.verify|verify} messages.
         * @param message OperateLuckyEndResp message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encode(message: game.IOperateLuckyEndResp, writer?: protobuf.Writer): protobuf.Writer;

        /**
         * Encodes the specified OperateLuckyEndResp message, length delimited. Does not implicitly {@link game.OperateLuckyEndResp.verify|verify} messages.
         * @param message OperateLuckyEndResp message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encodeDelimited(message: game.IOperateLuckyEndResp, writer?: protobuf.Writer): protobuf.Writer;

        /**
         * Decodes an OperateLuckyEndResp message from the specified reader or buffer.
         * @param reader Reader or buffer to decode from
         * @param [length] Message length if known beforehand
         * @returns OperateLuckyEndResp
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {protobuf.util.ProtocolError} If required fields are missing
         */
        public static decode(reader: (protobuf.Reader|Uint8Array), length?: number): game.OperateLuckyEndResp;

        /**
         * Decodes an OperateLuckyEndResp message from the specified reader or buffer, length delimited.
         * @param reader Reader or buffer to decode from
         * @returns OperateLuckyEndResp
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {protobuf.util.ProtocolError} If required fields are missing
         */
        public static decodeDelimited(reader: (protobuf.Reader|Uint8Array)): game.OperateLuckyEndResp;

        /**
         * Verifies an OperateLuckyEndResp message.
         * @param message Plain object to verify
         * @returns `null` if valid, otherwise the reason why it is not
         */
        public static verify(message: { [k: string]: any }): (string|null);

        /**
         * Creates an OperateLuckyEndResp message from a plain object. Also converts values to their respective internal types.
         * @param object Plain object
         * @returns OperateLuckyEndResp
         */
        public static fromObject(object: { [k: string]: any }): game.OperateLuckyEndResp;

        /**
         * Creates a plain object from an OperateLuckyEndResp message. Also converts values to other types if specified.
         * @param message OperateLuckyEndResp
         * @param [options] Conversion options
         * @returns Plain object
         */
        public static toObject(message: game.OperateLuckyEndResp, options?: protobuf.IConversionOptions): { [k: string]: any };

        /**
         * Converts this OperateLuckyEndResp to JSON.
         * @returns JSON object
         */
        public toJSON(): { [k: string]: any };
    }

    /** Properties of an OperateFlyReq. */
    interface IOperateFlyReq {

        /** OperateFlyReq fInd */
        fInd?: (number|null);
    }

    /** Represents an OperateFlyReq. */
    class OperateFlyReq implements IOperateFlyReq {

        /**
         * Constructs a new OperateFlyReq.
         * @param [properties] Properties to set
         */
        constructor(properties?: game.IOperateFlyReq);

        /** OperateFlyReq fInd. */
        public fInd: number;

        /**
         * Creates a new OperateFlyReq instance using the specified properties.
         * @param [properties] Properties to set
         * @returns OperateFlyReq instance
         */
        public static create(properties?: game.IOperateFlyReq): game.OperateFlyReq;

        /**
         * Encodes the specified OperateFlyReq message. Does not implicitly {@link game.OperateFlyReq.verify|verify} messages.
         * @param message OperateFlyReq message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encode(message: game.IOperateFlyReq, writer?: protobuf.Writer): protobuf.Writer;

        /**
         * Encodes the specified OperateFlyReq message, length delimited. Does not implicitly {@link game.OperateFlyReq.verify|verify} messages.
         * @param message OperateFlyReq message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encodeDelimited(message: game.IOperateFlyReq, writer?: protobuf.Writer): protobuf.Writer;

        /**
         * Decodes an OperateFlyReq message from the specified reader or buffer.
         * @param reader Reader or buffer to decode from
         * @param [length] Message length if known beforehand
         * @returns OperateFlyReq
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {protobuf.util.ProtocolError} If required fields are missing
         */
        public static decode(reader: (protobuf.Reader|Uint8Array), length?: number): game.OperateFlyReq;

        /**
         * Decodes an OperateFlyReq message from the specified reader or buffer, length delimited.
         * @param reader Reader or buffer to decode from
         * @returns OperateFlyReq
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {protobuf.util.ProtocolError} If required fields are missing
         */
        public static decodeDelimited(reader: (protobuf.Reader|Uint8Array)): game.OperateFlyReq;

        /**
         * Verifies an OperateFlyReq message.
         * @param message Plain object to verify
         * @returns `null` if valid, otherwise the reason why it is not
         */
        public static verify(message: { [k: string]: any }): (string|null);

        /**
         * Creates an OperateFlyReq message from a plain object. Also converts values to their respective internal types.
         * @param object Plain object
         * @returns OperateFlyReq
         */
        public static fromObject(object: { [k: string]: any }): game.OperateFlyReq;

        /**
         * Creates a plain object from an OperateFlyReq message. Also converts values to other types if specified.
         * @param message OperateFlyReq
         * @param [options] Conversion options
         * @returns Plain object
         */
        public static toObject(message: game.OperateFlyReq, options?: protobuf.IConversionOptions): { [k: string]: any };

        /**
         * Converts this OperateFlyReq to JSON.
         * @returns JSON object
         */
        public toJSON(): { [k: string]: any };
    }

    /** Properties of an OperateFlyResp. */
    interface IOperateFlyResp {

        /** OperateFlyResp nPos */
        nPos?: (number|null);

        /** OperateFlyResp fInd */
        fInd?: (number|null);

        /** OperateFlyResp nStep */
        nStep?: (number|null);

        /** OperateFlyResp nDesti */
        nDesti?: (number|null);

        /** OperateFlyResp res */
        res?: (number|null);
    }

    /** Represents an OperateFlyResp. */
    class OperateFlyResp implements IOperateFlyResp {

        /**
         * Constructs a new OperateFlyResp.
         * @param [properties] Properties to set
         */
        constructor(properties?: game.IOperateFlyResp);

        /** OperateFlyResp nPos. */
        public nPos: number;

        /** OperateFlyResp fInd. */
        public fInd: number;

        /** OperateFlyResp nStep. */
        public nStep: number;

        /** OperateFlyResp nDesti. */
        public nDesti: number;

        /** OperateFlyResp res. */
        public res: number;

        /**
         * Creates a new OperateFlyResp instance using the specified properties.
         * @param [properties] Properties to set
         * @returns OperateFlyResp instance
         */
        public static create(properties?: game.IOperateFlyResp): game.OperateFlyResp;

        /**
         * Encodes the specified OperateFlyResp message. Does not implicitly {@link game.OperateFlyResp.verify|verify} messages.
         * @param message OperateFlyResp message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encode(message: game.IOperateFlyResp, writer?: protobuf.Writer): protobuf.Writer;

        /**
         * Encodes the specified OperateFlyResp message, length delimited. Does not implicitly {@link game.OperateFlyResp.verify|verify} messages.
         * @param message OperateFlyResp message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encodeDelimited(message: game.IOperateFlyResp, writer?: protobuf.Writer): protobuf.Writer;

        /**
         * Decodes an OperateFlyResp message from the specified reader or buffer.
         * @param reader Reader or buffer to decode from
         * @param [length] Message length if known beforehand
         * @returns OperateFlyResp
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {protobuf.util.ProtocolError} If required fields are missing
         */
        public static decode(reader: (protobuf.Reader|Uint8Array), length?: number): game.OperateFlyResp;

        /**
         * Decodes an OperateFlyResp message from the specified reader or buffer, length delimited.
         * @param reader Reader or buffer to decode from
         * @returns OperateFlyResp
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {protobuf.util.ProtocolError} If required fields are missing
         */
        public static decodeDelimited(reader: (protobuf.Reader|Uint8Array)): game.OperateFlyResp;

        /**
         * Verifies an OperateFlyResp message.
         * @param message Plain object to verify
         * @returns `null` if valid, otherwise the reason why it is not
         */
        public static verify(message: { [k: string]: any }): (string|null);

        /**
         * Creates an OperateFlyResp message from a plain object. Also converts values to their respective internal types.
         * @param object Plain object
         * @returns OperateFlyResp
         */
        public static fromObject(object: { [k: string]: any }): game.OperateFlyResp;

        /**
         * Creates a plain object from an OperateFlyResp message. Also converts values to other types if specified.
         * @param message OperateFlyResp
         * @param [options] Conversion options
         * @returns Plain object
         */
        public static toObject(message: game.OperateFlyResp, options?: protobuf.IConversionOptions): { [k: string]: any };

        /**
         * Converts this OperateFlyResp to JSON.
         * @returns JSON object
         */
        public toJSON(): { [k: string]: any };
    }

    /** Properties of a BroadcastRoomInfo. */
    interface IBroadcastRoomInfo {

        /** BroadcastRoomInfo operate */
        operate?: (number|null);

        /** BroadcastRoomInfo uId */
        uId?: (string|null);

        /** BroadcastRoomInfo nick */
        nick?: (string|null);

        /** BroadcastRoomInfo photo */
        photo?: (string|null);

        /** BroadcastRoomInfo nPos */
        nPos?: (number|null);

        /** BroadcastRoomInfo skin */
        skin?: (string|null);
    }

    /** Represents a BroadcastRoomInfo. */
    class BroadcastRoomInfo implements IBroadcastRoomInfo {

        /**
         * Constructs a new BroadcastRoomInfo.
         * @param [properties] Properties to set
         */
        constructor(properties?: game.IBroadcastRoomInfo);

        /** BroadcastRoomInfo operate. */
        public operate: number;

        /** BroadcastRoomInfo uId. */
        public uId: string;

        /** BroadcastRoomInfo nick. */
        public nick: string;

        /** BroadcastRoomInfo photo. */
        public photo: string;

        /** BroadcastRoomInfo nPos. */
        public nPos: number;

        /** BroadcastRoomInfo skin. */
        public skin: string;

        /**
         * Creates a new BroadcastRoomInfo instance using the specified properties.
         * @param [properties] Properties to set
         * @returns BroadcastRoomInfo instance
         */
        public static create(properties?: game.IBroadcastRoomInfo): game.BroadcastRoomInfo;

        /**
         * Encodes the specified BroadcastRoomInfo message. Does not implicitly {@link game.BroadcastRoomInfo.verify|verify} messages.
         * @param message BroadcastRoomInfo message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encode(message: game.IBroadcastRoomInfo, writer?: protobuf.Writer): protobuf.Writer;

        /**
         * Encodes the specified BroadcastRoomInfo message, length delimited. Does not implicitly {@link game.BroadcastRoomInfo.verify|verify} messages.
         * @param message BroadcastRoomInfo message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encodeDelimited(message: game.IBroadcastRoomInfo, writer?: protobuf.Writer): protobuf.Writer;

        /**
         * Decodes a BroadcastRoomInfo message from the specified reader or buffer.
         * @param reader Reader or buffer to decode from
         * @param [length] Message length if known beforehand
         * @returns BroadcastRoomInfo
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {protobuf.util.ProtocolError} If required fields are missing
         */
        public static decode(reader: (protobuf.Reader|Uint8Array), length?: number): game.BroadcastRoomInfo;

        /**
         * Decodes a BroadcastRoomInfo message from the specified reader or buffer, length delimited.
         * @param reader Reader or buffer to decode from
         * @returns BroadcastRoomInfo
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {protobuf.util.ProtocolError} If required fields are missing
         */
        public static decodeDelimited(reader: (protobuf.Reader|Uint8Array)): game.BroadcastRoomInfo;

        /**
         * Verifies a BroadcastRoomInfo message.
         * @param message Plain object to verify
         * @returns `null` if valid, otherwise the reason why it is not
         */
        public static verify(message: { [k: string]: any }): (string|null);

        /**
         * Creates a BroadcastRoomInfo message from a plain object. Also converts values to their respective internal types.
         * @param object Plain object
         * @returns BroadcastRoomInfo
         */
        public static fromObject(object: { [k: string]: any }): game.BroadcastRoomInfo;

        /**
         * Creates a plain object from a BroadcastRoomInfo message. Also converts values to other types if specified.
         * @param message BroadcastRoomInfo
         * @param [options] Conversion options
         * @returns Plain object
         */
        public static toObject(message: game.BroadcastRoomInfo, options?: protobuf.IConversionOptions): { [k: string]: any };

        /**
         * Converts this BroadcastRoomInfo to JSON.
         * @returns JSON object
         */
        public toJSON(): { [k: string]: any };
    }

    /** Properties of a BroadcastOperateInfo. */
    interface IBroadcastOperateInfo {

        /** BroadcastOperateInfo operate */
        operate?: (number|null);

        /** BroadcastOperateInfo uId */
        uId?: (string|null);

        /** BroadcastOperateInfo nick */
        nick?: (string|null);

        /** BroadcastOperateInfo photo */
        photo?: (string|null);

        /** BroadcastOperateInfo nPos */
        nPos?: (number|null);
    }

    /** Represents a BroadcastOperateInfo. */
    class BroadcastOperateInfo implements IBroadcastOperateInfo {

        /**
         * Constructs a new BroadcastOperateInfo.
         * @param [properties] Properties to set
         */
        constructor(properties?: game.IBroadcastOperateInfo);

        /** BroadcastOperateInfo operate. */
        public operate: number;

        /** BroadcastOperateInfo uId. */
        public uId: string;

        /** BroadcastOperateInfo nick. */
        public nick: string;

        /** BroadcastOperateInfo photo. */
        public photo: string;

        /** BroadcastOperateInfo nPos. */
        public nPos: number;

        /**
         * Creates a new BroadcastOperateInfo instance using the specified properties.
         * @param [properties] Properties to set
         * @returns BroadcastOperateInfo instance
         */
        public static create(properties?: game.IBroadcastOperateInfo): game.BroadcastOperateInfo;

        /**
         * Encodes the specified BroadcastOperateInfo message. Does not implicitly {@link game.BroadcastOperateInfo.verify|verify} messages.
         * @param message BroadcastOperateInfo message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encode(message: game.IBroadcastOperateInfo, writer?: protobuf.Writer): protobuf.Writer;

        /**
         * Encodes the specified BroadcastOperateInfo message, length delimited. Does not implicitly {@link game.BroadcastOperateInfo.verify|verify} messages.
         * @param message BroadcastOperateInfo message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encodeDelimited(message: game.IBroadcastOperateInfo, writer?: protobuf.Writer): protobuf.Writer;

        /**
         * Decodes a BroadcastOperateInfo message from the specified reader or buffer.
         * @param reader Reader or buffer to decode from
         * @param [length] Message length if known beforehand
         * @returns BroadcastOperateInfo
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {protobuf.util.ProtocolError} If required fields are missing
         */
        public static decode(reader: (protobuf.Reader|Uint8Array), length?: number): game.BroadcastOperateInfo;

        /**
         * Decodes a BroadcastOperateInfo message from the specified reader or buffer, length delimited.
         * @param reader Reader or buffer to decode from
         * @returns BroadcastOperateInfo
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {protobuf.util.ProtocolError} If required fields are missing
         */
        public static decodeDelimited(reader: (protobuf.Reader|Uint8Array)): game.BroadcastOperateInfo;

        /**
         * Verifies a BroadcastOperateInfo message.
         * @param message Plain object to verify
         * @returns `null` if valid, otherwise the reason why it is not
         */
        public static verify(message: { [k: string]: any }): (string|null);

        /**
         * Creates a BroadcastOperateInfo message from a plain object. Also converts values to their respective internal types.
         * @param object Plain object
         * @returns BroadcastOperateInfo
         */
        public static fromObject(object: { [k: string]: any }): game.BroadcastOperateInfo;

        /**
         * Creates a plain object from a BroadcastOperateInfo message. Also converts values to other types if specified.
         * @param message BroadcastOperateInfo
         * @param [options] Conversion options
         * @returns Plain object
         */
        public static toObject(message: game.BroadcastOperateInfo, options?: protobuf.IConversionOptions): { [k: string]: any };

        /**
         * Converts this BroadcastOperateInfo to JSON.
         * @returns JSON object
         */
        public toJSON(): { [k: string]: any };
    }

    /** Properties of an example. */
    interface Iexample {

        /** example cityId */
        cityId?: (string|null);

        /** example info */
        info?: (game.ILoginResp|null);

        /** example bo */
        bo?: (boolean|null);

        /** example buildingId */
        buildingId?: (string[]|null);

        /** example soldierType */
        soldierType?: (number|null);
    }

    /** Represents an example. */
    class example implements Iexample {

        /**
         * Constructs a new example.
         * @param [properties] Properties to set
         */
        constructor(properties?: game.Iexample);

        /** example cityId. */
        public cityId: string;

        /** example info. */
        public info?: (game.ILoginResp|null);

        /** example bo. */
        public bo: boolean;

        /** example buildingId. */
        public buildingId: string[];

        /** example soldierType. */
        public soldierType: number;

        /**
         * Creates a new example instance using the specified properties.
         * @param [properties] Properties to set
         * @returns example instance
         */
        public static create(properties?: game.Iexample): game.example;

        /**
         * Encodes the specified example message. Does not implicitly {@link game.example.verify|verify} messages.
         * @param message example message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encode(message: game.Iexample, writer?: protobuf.Writer): protobuf.Writer;

        /**
         * Encodes the specified example message, length delimited. Does not implicitly {@link game.example.verify|verify} messages.
         * @param message example message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encodeDelimited(message: game.Iexample, writer?: protobuf.Writer): protobuf.Writer;

        /**
         * Decodes an example message from the specified reader or buffer.
         * @param reader Reader or buffer to decode from
         * @param [length] Message length if known beforehand
         * @returns example
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {protobuf.util.ProtocolError} If required fields are missing
         */
        public static decode(reader: (protobuf.Reader|Uint8Array), length?: number): game.example;

        /**
         * Decodes an example message from the specified reader or buffer, length delimited.
         * @param reader Reader or buffer to decode from
         * @returns example
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {protobuf.util.ProtocolError} If required fields are missing
         */
        public static decodeDelimited(reader: (protobuf.Reader|Uint8Array)): game.example;

        /**
         * Verifies an example message.
         * @param message Plain object to verify
         * @returns `null` if valid, otherwise the reason why it is not
         */
        public static verify(message: { [k: string]: any }): (string|null);

        /**
         * Creates an example message from a plain object. Also converts values to their respective internal types.
         * @param object Plain object
         * @returns example
         */
        public static fromObject(object: { [k: string]: any }): game.example;

        /**
         * Creates a plain object from an example message. Also converts values to other types if specified.
         * @param message example
         * @param [options] Conversion options
         * @returns Plain object
         */
        public static toObject(message: game.example, options?: protobuf.IConversionOptions): { [k: string]: any };

        /**
         * Converts this example to JSON.
         * @returns JSON object
         */
        public toJSON(): { [k: string]: any };
    }
}
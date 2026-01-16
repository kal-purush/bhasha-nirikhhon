import { AggregateRoot } from '../../../Shared/domain/AggregateRoot';
import { Nullable } from '../../../Shared/domain/Nullable';
import { Balance } from '../../../Shared/domain/value-object/Balance';
import { TagId } from '../../Shared/domain/Tag/TagId';
import { UserId } from '../../Shared/domain/User/UserId';
import { TagDescription } from './TagDescription';
import { TagName } from './TagName';

export default class Tag extends AggregateRoot {
    readonly id: TagId;
    readonly userId: UserId;
    readonly parentTagId: Nullable<TagId>;
    private _name: TagName;
    private _description: TagDescription;
    private _balance: Balance;
    private _netBalance: Balance;


    constructor(tagId: TagId,
                user: UserId,
                parentTag: Nullable<TagId>,
                name: TagName,
                description: TagDescription,
                balance: Balance,
                netBalance: Balance) {
        super();
        this.id = tagId;
        this.userId = user;
        this.parentTagId = parentTag;
        this._name = name;
        this._description = description;
        this._balance = balance;
        this._netBalance = netBalance;
    }

    public get name(): TagName {
        return this._name;
    }

    public set name(value: TagName) {
        this._name = value;
    }

    public get description(): TagDescription {
        return this._description;
    }

    public set description(value: TagDescription) {
        this._description = value;
    }

    public get balance(): Balance {
        return this._balance;
    }

    public set balance(value: Balance) {
        this._balance = value;
    }

    public get netBalance(): Balance {
        return this._netBalance;
    }

    public set netBalance(value: Balance) {
        this._netBalance = value;
    }

    static create(tagId: TagId,
                  user: UserId,
                  parentTag: Nullable<TagId>,
                  name: TagName,
                  description: TagDescription,
                  balance: Balance,
                  netBalance: Balance): Tag {
        return new Tag(tagId, user, parentTag, name, description, balance, netBalance);
    }

    static fromPrimitives(plainData: {
        id: string;
        userId: string;
        parentTagId: string | null;
        name: string;
        description: string;
        balance: number;
        netBalance: number;
    }): Tag {
        return new Tag(
            new TagId(plainData.id),
            new UserId(plainData.userId),
            plainData.parentTagId ? new TagId(plainData.parentTagId) : null,
            new TagName(plainData.name),
            new TagDescription(plainData.description),
            new Balance(plainData.balance),
            new Balance(plainData.netBalance)
        );
    }

    public toPrimitives() {
        return {
            id: this.id.value,
            userId: this.userId.value,
            parentTagId: this.parentTagId?.value,
            name: this._name.value,
            description: this._description.value,
            balance: this._balance.value,
            netBalance: this._netBalance.value
        };
    }
}
interface IAuthMessage
{
	Username: string;
}

interface ICheckAuthMessage
{
	Success: boolean;
}

interface ILoginMessage extends IAuthMessage
{
	Colour: string;
	Status: string;
	State: States;
	Password?: string;
	HashedPassword?: string;
}

interface ILoginResponse extends ILoginMessage
{
	Success: boolean;
	Message?: string;
	Users: IUser[];
}

interface IMessageMessage extends IAuthMessage
{
	Message: string;
	Timestamp: string;
	MessageId: string;
	Type: SendTypes;
}

interface IPrivateMessage extends IMessageMessage
{
	To: string;
}

interface IStateChangedMessage extends IAuthMessage
{
	State: States;
	TimeStamp: string;
}

interface ITypingMessage extends IAuthMessage
{
	To: string;
}

interface ISeenMessage extends IAuthMessage
{
	To: string;
	MessageId: string;
}

interface IUser
{
	Username: string;
	State: States;
	Colour: string;
	Status?: string;
	LastSeen?: string;
	Host?: string;
	Socket?: any;
	LastMessage?: Date;
}

interface IConnectionChangedMessage extends IAuthMessage
{
	Users: IUser[];
}

interface ICallResult<T>
{
	Success: boolean;
	Result: T;
	Error: string;
}

interface IDebugInfoRequest
{
	InfoType: DebugInfoTypes;
}

interface ILastActiveResult
{
	Username: string;
	LastActive: string;
}

interface ILastActiveMessage
{
	Results: ILastActiveResult[];
}
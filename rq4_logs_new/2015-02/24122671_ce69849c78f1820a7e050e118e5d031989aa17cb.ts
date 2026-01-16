declare module com.truckmuncher.api.auth {
	interface ProtoBufModel {
		toArrayBuffer(): ArrayBuffer;
		//toBuffer(): NodeBuffer;
		//encode(): ByteBuffer;
		toBase64(): string;
		toString(): string;
	}

	export interface ProtoBufBuilder {
		AuthRequest: AuthRequestBuilder;
		AuthResponse: AuthResponseBuilder;
		DeleteAuthResponse: DeleteAuthResponseBuilder;
		
	}
}

declare module com.truckmuncher.api.auth {

	export interface AuthRequest extends ProtoBufModel {
		
	}
	
	export interface AuthRequestBuilder {
		new(): AuthRequest;
		decode(buffer: ArrayBuffer) : AuthRequest;
		//decode(buffer: NodeBuffer) : AuthRequest;
		//decode(buffer: ByteArrayBuffer) : AuthRequest;
		decode64(buffer: string) : AuthRequest;
		
	}	
}

declare module com.truckmuncher.api.auth {

	export interface AuthResponse extends ProtoBufModel {
		userId: string;
		getUserId() : string;
		setUserId(userId : string): void;
		username: string;
		getUsername() : string;
		setUsername(username : string): void;
		sessionToken: string;
		getSessionToken() : string;
		setSessionToken(sessionToken : string): void;
		
	}
	
	export interface AuthResponseBuilder {
		new(): AuthResponse;
		decode(buffer: ArrayBuffer) : AuthResponse;
		//decode(buffer: NodeBuffer) : AuthResponse;
		//decode(buffer: ByteArrayBuffer) : AuthResponse;
		decode64(buffer: string) : AuthResponse;
		
	}	
}

declare module com.truckmuncher.api.auth {

	export interface DeleteAuthResponse extends ProtoBufModel {
		
	}
	
	export interface DeleteAuthResponseBuilder {
		new(): DeleteAuthResponse;
		decode(buffer: ArrayBuffer) : DeleteAuthResponse;
		//decode(buffer: NodeBuffer) : DeleteAuthResponse;
		//decode(buffer: ByteArrayBuffer) : DeleteAuthResponse;
		decode64(buffer: string) : DeleteAuthResponse;
		
	}	
}

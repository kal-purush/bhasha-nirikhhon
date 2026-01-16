/// <reference path="./guid" />
/// <reference path="./random" />
/// <reference path="./storage" />
/// <reference path="../core/models/user" />

module Utils{
	
	import UserModel = Core.Models.User;
	
	export class Session{
		
		private user:UserModel;
		
		private CHAT_USER:string = 'chat_user';
		
		private guid:Guid;
		private random:Random;
		private storage:Storage;
		
		constructor()
		{
			this.guid = new Guid();
			this.random = new Random();
			this.storage = new Storage();
		}
		
		public login():UserModel{
			if(!this.storage.has(this.CHAT_USER)){
				this.user = this.new();
			}else{
				this.user = this.get();
			}
			
			this.user.on('change', (user)=>{	
				this.storage.set(this.CHAT_USER, 
					JSON.stringify(user.toJSON()));
			});
			
			return this.user;
		}
		
		private new():UserModel{
			
			this.user = new UserModel({
				uuid: this.guid.new(),
				username: this.random.username()
			});
	
			this.storage.set(this.CHAT_USER, 
				JSON.stringify(this.user.toJSON()));
				
			return this.user; 			
		}
		
		private get():UserModel{
			
			this.user = new UserModel(JSON.parse(
				this.storage.get(this.CHAT_USER)
			));
			
			return this.user;
		}
		
	}
	
}
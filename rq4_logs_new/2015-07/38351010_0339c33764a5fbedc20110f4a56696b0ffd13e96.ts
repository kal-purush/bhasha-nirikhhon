/// <reference path="../../../typings/backbone/backbone.d.ts" />
/// <reference path="../../core/models/message" />
/// <reference path="../../utils/session" />

module Views.Messages{
	
	export class MessageListView extends Backbone.View<Core.Models.Message>{
		
		$textarea:any;
		
		constructor(options:Backbone.ViewOptions<Core.Models.Message>){
			
			super(options);
			this.$textarea =  this.$el.parent().find('textarea');
			
			this.listenTo(this.collection, 'add', this.onAdd);
			
			this.$textarea.keypress((args)=>{
				this.onKeyPress(args);
			});
		}
		
		onAdd(model){
			
			let username =  model.get("sender").get("username");
			let text = model.get("text");
			let datetime = model.get("datetime");
			
			let template:string = `<li>
				<figure>
					<img src="https://robohash.org/${username}.png" 
					title="${username}"/>
				</figure>
				<strong>${username}</strong> 
				<small>${datetime.toLocaleTimeString()}</small> <br> 
				<span> ${text} </span>
			</li>`;
			
			this.$el.append(template);
		}
		
		onKeyPress(e:JQueryKeyEventObject){
			
			if(e.keyCode==13){	
				e.preventDefault();
				
				let session = new Session();
				
				if(this.$textarea.val().trim().length==0){
					return;
				}
				
				let date:Date = new Date();
				
				this.collection.add(new MessageModel({
					sender: session.login(),
					text : this.$textarea.val(),
					datetime: date
				}));
				
				this.$textarea.val('');
			}
		}
		
	}
	
}
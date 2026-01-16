RequireScript("Dialog.js");

function ConversationManager(player) {
	this.Player = player;
	this.NPC = "";
	this.Speaker = "";
	this.Dialog = "";
	this.SentenceId = 0;
	this.ConversationStarted = false;
	this.IsWaitingForKey = false;
	
	var date = new Date();
	this.ConversationEndTime = date.getTime();
}

ConversationManager.prototype.UpdateScripts = function() {
	if(this.ConversationStarted) {
		this.Speaker = IsPlayerSpeaking(this.SentenceId) ? this.Player : this.NPC;
		windowManager.ShowDialog(this.Speaker,GetSentence(this.SentenceId));
		TriggerUpdateConversation();
		
		if(!IsKeyPressed(gameManager.GameSetting.KeyA)) {
			this.IsWaitingForKey = true;
		}
	}
}

ConversationManager.prototype.StartConversation = function(npc,dialog) {
	this.NPC = npc;
	this.Dialog = dialog;
	this.SentenceId = GetSentenceIdByDialog(dialog);
	this.ConversationStarted = true;
}

ConversationManager.prototype.EndConversation = function() {
	this.NPC = "";
	this.Speaker = "";
	this.Dialog = "";
	this.SentenceId = 0;
	this.ConversationStarted = false;
	
	var date = new Date();
	this.ConversationEndTime = date.getTime();
}

ConversationManager.prototype.UpdateConversation = function() {
	if(IsLastSentence(this.SentenceId)) {
		TriggerEndConversation();
	} else {
		this.SentenceId = GetNextSentenceId(this.SentenceId);
	}
	
	this.IsWaitingForKey = false;
}

ConversationManager.prototype.IsConversationEnabled = function() {
	var date = new Date();
	var timeInterval = date.getTime() - this.ConversationEndTime;
	var isEnabled = (timeInterval>1000) ? true : false;
	return isEnabled;
}
var a1 = new ReactiveVar();
var a2 = new ReactiveVar();
var a3 = new ReactiveVar();
var check = new ReactiveVar();
var correct = new ReactiveVar();
var counter = new ReactiveVar();
var corCounter = new ReactiveVar();
var numCheck = new ReactiveVar();
var input= new ReactiveVar();

//function to generate a new numbers for a question.  Also resets the status of numCheck so the user isn't asked to enter a number, and resets check to false so the correct buttons are shown. 
function picker()
{
	numCheck.set("yes");
	check.set(false);
	a1.set(Math.floor(Math.random()*30)+2);
	a2.set(Math.floor(Math.random()*30)+2);
	a3.set(a1.get()+a2.get());
	
};

Template.Addition.helpers(
{
	check: function()
	{
		return check.get();
	},
	
	correct: function()
	{
		return correct.get();
	},
	
	A1:function()
	{
		return a1.get();
	},
	
	A2:function()
	{
		return a2.get()	;
	},
	
	A3:function()
	{
		return a3.get()	;
	},
	result:function()
	{
		//If the total number of questions answered is greater than 0, then return the number of correctly answered questions followed by a slash and the total number of questions asked
		if(counter.get()>0)
		{
			return corCounter.get()+"/"+counter.get();
		}
		else
		{
			return ""
		}
	},
	
	num:function()
	{
		//If the user enters something that is not a number, or the entry is blank, return Please enter..... otherwise displays nothing.  
		if(numCheck.get()==="no")
		{
			return "Please enter a number.";
		}
		else 
		{
			return "";
		}
	},
});

Template.Addition.events({
//When clicking element with id Submit (i.e. the button Check!)
'submit #answer':function()
	{
		//Prevents the form refreshing the page when submitted
		event.preventDefault();
		
		//grabs the value of the text box)
		input.set(document.getElementById("ans").value);
			
		//If the answer is not a number or if the text box is empty
		if(isNaN(input.get())===true||input.get()==="")
		{
			//sets numCheck to no, prompting the Please enter a number message
			numCheck.set("no");
			console.log(numCheck.get());
			
		}
		//if the entry is a number, then sets numCheck to yes, so that it returns nothing, adds 1 to the total counter, and sets check to true so that the template shows correct/incorrect and the correct buttons
		else if(isNaN(input.get())===false)
		{
			numCheck.set("yes");
			check.set(true);
			counter.set(counter.get()+1);
			
			//If the input is equal to the answer i.e. they've answered correctly, add one to the correct counter, and set correct to display "Correct!" 
			if (input.get()==a3.get())
				{
					corCounter.set(corCounter.get()+1);
					correct.set("Correct!");
				}
			
			//Otherwise, set correct to display incorrect and the correct answer
			else
				{
					correct.set("Incorrect! The answer is "+a3.get()+".");
				};
		}
	},
	
/*'click #roll':function()
	{
		picker();
		console.log("HI");
	},*/

//On clicking the next question button, call function picker
'click #next':function()
	{
		picker();
	},
});

//When the template is rendered, reset both the Counter and Correct Counters to zero, and call picker 

Template.Addition.onRendered(function()
{
	picker();
	counter.set(0);
	corCounter.set(0);
	document.getElementById("ans").focus();
});
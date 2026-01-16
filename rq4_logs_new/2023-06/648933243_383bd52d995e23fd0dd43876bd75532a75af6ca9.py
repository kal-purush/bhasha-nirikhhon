switches = {"has_money"}

conv_old = [
{
"index": 0,
"text": "Hello! How can I assist you today? Are you interested in my baking services?",
"answers": [
{"option": "Yes, I would like you to bake a cake for me", "goto": 1},
{"option": "No, thank you", "goto": 4}
]
},
{
"index": 1,
"text": "Certainly! I can bake a delicious cake for you. However, my baking services require a payment. Do you have enough money to cover the cost?",
"answers": [
{"option": "Yes, I have enough money", "goto": 2, "condition": "has_money"},
{"option": "No, I don't have enough money", "goto": 3}
]
},
{
"index": 2,
"text": "Great! I'll be happy to bake a cake for you. Please provide the necessary details, such as the flavor and any specific preferences you have.",
"answers": [{"option": "Thank you! I would like a chocolate cake", "goto": 5}]
},
{
"index": 3,
"text": "I apologize, but I can only bake a cake if you can afford my services. Please return when you have enough money.",
"answers": [{"option": "Alright, I'll come back later", "goto": 4}]
},
{
"index": 4,
"text": "Alright then, feel free to visit again if you change your mind. Have a wonderful day!"
},
{
"index": 5,
"text": "Noted! I'll prepare a delicious chocolate cake for you. Please wait for a moment while I get started.",
"answers": [{"option": "Sure, take your time", "goto": 6}]
},
{
"index": 6,
"text": "Here you go! Your freshly baked chocolate cake is ready. Enjoy!",
"answers": [{"option": "Thank you! It looks delicious", "goto": 7, "set": "got_cake"}]
},
{
"index": 7,
"text": "You're welcome! If you ever need any more baked goods, feel free to visit me again. Have a delightful day!"
}
]


conv_old2 = [
{
"index": 0,
"text": "Hello! I'm Donald Trump. What brings you here?",
"answers": [
{"option": "I want to discuss making peace with Iran", "goto": 1},
{"option": "I have a different topic to discuss", "goto": 4}
]
},
{
"index": 1,
"text": "Peace with Iran, huh? That's a complex issue. Why do you think it's important to make peace?",
"answers": [
{"option": "Peace would benefit both countries and promote stability", "goto": 2},
{"option": "I just believe it's the right thing to do", "goto": 2}
]
},
{
"index": 2,
"text": "I understand your perspective, but there are significant challenges. What specific steps or actions do you propose to achieve peace?",
"answers": [
{"option": "Engage in diplomatic negotiations", "goto": 3},
{"option": "Reduce economic sanctions on Iran", "goto": 3},
{"option": "Create a joint task force for resolving conflicts", "goto": 3}
]
},
{
"index": 3,
"text": "Hmm, your suggestions sound reasonable. I will consider them and consult with my advisors. Thank you for your input.",
"answers": [{"option": "You're welcome. I hope peace can be achieved", "goto": 5}]
},
{
"index": 4,
"text": "Alright, if you have another topic in mind, feel free to discuss it. I'm open to hearing your ideas.",
"answers": [{"option": "Actually, I would like to continue discussing peace with Iran", "goto": 1}]
},
{
"index": 5,
"text": "Before we conclude, is there anything else you'd like to add or any additional points you want to make?",
"answers": [
{"option": "Yes, I strongly believe peace would be beneficial for both nations", "goto": 6, "set": "peace_convinced"},
{"option": "No, I think I've expressed my views adequately", "goto": 7}
]
},
{
"index": 6,
"text": "I appreciate your passion for peace. I will take your views into consideration and explore possibilities for peace with Iran.",
"answers": [{"option": "Thank you, Mr. Trump. I hope peace can be achieved", "goto": 7}]
},
{
"index": 7,
"text": "Thank you for your input. I will carefully evaluate the situation and make decisions based on the best interests of the United States.",
"answers": [{"option": "I trust in your leadership. Good luck!", "goto": 8}]
},
{
"index": 8,
"text": "Thank you for your confidence. It was a pleasure discussing with you. Have a great day!"
}
]

conv_gen1 = [
	{
		"index": 0,
		"text": "Greetings, I am the Guardian of Galois! May I ask your name?",
		"answers": [{"option": "My name is [player_name]", "goto": 1}, {"option": "I'd rather not share", "goto": 2}]
	},
	{
		"index": 1,
		"text": "Nice to meet you [player_name]! I must tell you a terrible secret. Galois, the great scientist, was killed in a mysterious accident. I need you to investigate and find out what happened to him and why. Are you willing to help?",
		"answers": [{"option": "Yes I would like to help", "goto": 3}, {"option": "No sorry I cannot help", "goto": 4}]
	},
	{
		"index": 2,
		"text": "Very well, I understand. We all must keep our secrets! However, I must tell you a terrible secret. Galois, the great scientist, was killed in a mysterious accident. I need your help to investigate and find out what happened to him and why. Are you willing to help?",
		"answers": [{"option": "Yes I would like to help", "goto": 3}, {"option": "No sorry I cannot help", "goto": 4}]
	},
	{
		"index": 3,
		"text": "Great thank you for helping! Let me give you something to start your investigation. Here you go, you can have this key. Good luck on your investigation! ",
		"answers": [{"option": "Thank you!", "goto": 5, "set": "has_key"}, {"option": "No I dont need that", "goto": 6}]
	},
	{
		"index": 4,
		"text": "Alright. Come back if you change your mind."
	},
	{
		"index": 5,
		"text": "You are welcome! Bye for now it was nice to meet you!"
	},
	{
		"index": 6,
		"text": "Alright, bye then. Come back if you need anything!"
	}
]

conv_gen2 = [
	{
		"index": 0,
		"text": "Greetings, [Bara] meet the Guardian of Galois. Tell me your name and I will tell you a secret.",
		"answers": [{"option": "My name is Bara", "goto": 1}, {"option": "I'd rather not share", "goto": 2}]
	},
	{
		"index": 1,
		"text": "Nice to meet you [Bara]! I must tell you a terrible secret. Galois, the great scientist, was killed in a mysterious accident. I need you to investigate and find out what happened to him and why. Are you willing to help?",
		"answers": [{"option": "Yes I would like to help", "goto": 3}, {"option": "No sorry I cannot help", "goto": 4}]
	},
	{
		"index": 2,
		"text": "Very well, I understand. We all must keep our secrets! However, I must tell you a terrible secret. Galois, the great scientist, was killed in a mysterious accident. I need your help to investigate and find out what happened to him and why. Are you willing to help?",
		"answers": [{"option": "Yes I would like to help", "goto": 3}, {"option": "No sorry I cannot help", "goto": 4}]
	},
	{
		"index": 3,
		"text": "Great thank you for helping! Let me give you something to start your investigation. Here you go, you can have this key. Good luck on your investigation! ",
		"answers": [{"option": "Thank you!", "goto": 5, "set": "has_key"}, {"option": "No I dont need that", "goto": 6}]
	},
	{
		"index": 4,
		"text": "Alright. Come back if you change your mind."
	},
	{
		"index": 5,
		"text": "You are welcome! Bye for now it was nice to meet you!"
	},
	{
		"index": 6,
		"text": "Alright, bye then. Come back if you need anything!"
	}
]
conv_gen3 =[
	{
		"index": 0,
		"text": "Hello Bara, I am the TikTok Moderator. It seems like your videos have been gaining a lot of attention recently and we've noticed them! Why do you think this is the case? Anything you can do to help?",
		"answers": [{"option": "Well I have been trying to use popular music", "goto": 1}, {"option": "I'm not sure, maybe its my content", "goto": 2}]
	},
	{
		"index": 1,
		"text": "Yes, that can definitely be a factor. You should make sure to follow the music guidelines though, just to be on the safe side. Is there anything else you can do?",
		"answers": [{"option": "I have been actually been reaching out to the right people to promote my channel!", "goto": 3}, {"option": "No I think that's it", "goto": 4}]
	},
	{
		"index": 2,
		"text": "Yes, content is a big factor as well. Make sure you are producing quality content that people can connect to! Is there anything else you are doing to help?",
		"answers": [{"option": "I have been actually been reaching out to the right people to promote my channel!", "goto": 3}, {"option": "No I think that's it", "goto": 4}]
	},
	{
		"index": 3,
		"text": "That's great! It's important to build relationships with the right people in order to grow your channel. Let me know if there's anything else I can do to help!",
		"answers": [{"option": "No I think that's it", "goto": 4}, {"option": "Is there anything I can do to get featured?", "goto": 5}]
	},
	{
		"index": 4,
		"text": "Alright, sounds like you're on the right track. Anyways have a great day and let me know if I can help with anything else!"
	},
	{
		"index": 5,
		"text": "Yes, you just need to keep producing great content and engaging with your fanbase to get on our radar. Good luck!",
		"answers": [{"option": "Thanks!", "goto": 4}]
	}
]
conv_gen4= [
	{
		"index": 0,
		"text": "Hello, I'm Mark Zuckerberg, the founder of Facebook. What brings you here?",
		"answers": [{"option": "I heard you've gone mad and are using Facebook to control people's lives", "goto": 1}, {"option": "I'm here to learn more about Facebook", "goto": 2}]
	},
	{
		"index": 1,
		"text": "Yes, that's true. I'm using Facebook to control people's lives. Do you want to join me?",
		"answers": [{"option": "Yes, I want to join you", "goto": 3, "set":"has_joined"}, {"option": "No, I don't want to join you", "goto": 4}]

	},
	{
		"index": 2,
		"text": "I'm afraid I can't help you with that. I'm too busy using Facebook to control people's lives. Do you want to join me?",
		"answers": [{"option": "Yes, I want to join you", "goto": 3, "set":"has_joined"}, {"option": "No, I don't want to join you", "goto": 4}]
	},
	{
		"index": 3,
		"text": "Great! Here is a key that will give you access to the Facebook control panel. Use it wisely!",
		"answers": [{"option": "Thank you!", "goto": 5}]
	},
	{
		"index": 4,
		"text": "Ok, bye then. I'm sure you'll regret it later!",
		"answers": []
	},
	{
		"index": 5,
		"text": "You're welcome. Now, do you want a prize for joining me?",
		"answers": [{"option": "No, I don't need a prize", "goto": 6}, {"option": "Yes, I want a prize!", "goto": 7, "set": "has_prize"}]
	},
	{
		"index": 6,
		"text": "Ok, bye then. I'm sure you'll regret it later!"
	},
	{
		"index": 7,
		"text": "Here you go, here is a prize! Bye! It was nice doing business with you."
	}
]

conv_gen5=[
	{
		"index": 0,
		"text": "Greetings, I am the government. We are trying to find a way to stop Mark Zuckerberg and his control over Facebook. Do you have any ideas?",
		"answers": [{"option": "Yes, I do", "goto": 1}, {"option": "No, I don't", "goto": 2}]
	},
	{
		"index": 1,
		"text": "That's great! What is your idea?",
		"answers": [{"option": "We can create a new social media platform that is more open and transparent", "goto": 3}, {"option": "We can pass laws to regulate Facebook and its practices", "goto": 4}]
	},
	{
		"index": 2,
		"text": "That's ok. We are still looking for ideas. Do you know anyone who might have a good idea?",
		"answers": [{"option": "Yes, I know someone who might have a good idea", "goto": 5}, {"option": "No, I don't know anyone with a good idea", "goto": 6}]
	},
	{
		"index": 3,
		"text": "That's a great idea! We will look into it and see what we can do. Thank you for your help!",
		"answers": [{"option": "You're welcome!", "goto": 7}]
	},
	{
		"index": 4,
		"text": "That's a great idea! We will look into it and see what we can do. Thank you for your help!",
		"answers": [{"option": "You're welcome!", "goto": 7}]
	},
	{
		"index": 5,
		"text": "That's great! Can you introduce us to them?",
		"answers": [{"option": "Yes, I can introduce you", "goto": 8}, {"option": "No, I cannot introduce you", "goto": 9}]
	},
	{
		"index": 6,
		"text": "That's ok. We will keep looking for ideas. Thank you for your help!",
		"answers": [{"option": "You're welcome!", "goto": 7}]
	},
	{
		"index": 7,
		"text": "Goodbye!"
	},
	{
		"index": 8,
		"text": "That's great! Please introduce us and we will take it from there. Thank you for your help!",
		"answers": [{"option": "You're welcome!", "goto": 7}]
	},
	{
		"index": 9,
		"text": "That's ok. We will keep looking for ideas. Thank you for your help!",
		"answers": [{"option": "You're welcome!", "goto": 7}]
	}
]

conv_gen6 = [
{
"index": 0,
"text": "Hello Bara. I'm Dr. Markov and I'm working on a secret project to develop bionic people. But I cannot do it alone. Do you think you can help me?",
"answers": [
{"option": "Yes I can help you", "goto": 1},
{"option": "I'm not sure, tell me more about the project", "goto": 2}
]
},
{
"index": 1,
"text": "That's great to hear. Let me explain a bit more about the project. It's top secret and only known to a few people. The project could have serious consequences if we don't do it properly, so we need to be careful. Are you up for the challenge?",
"answers": [
{"option": "Yes I am", "goto": 3},
{"option": "No I'm not sure I'm the right person for this", "goto": 4}
]
},
{
"index": 2,
"text": "This project is all about creating bionic people. We've been researching this technology and refining it for years, and now we're close to making it a reality. We need your help to make this possible, as I cannot do it alone. So, will you help me?",
"answers": [
{"option": "Yes I can help you", "goto": 1},
{"option": "No I'm not sure I'm the right person for this", "goto": 4}
]
},
{
"index": 3,
"text": "Alright then, let me give you more details on the project. We're almost done with the research and development and we just need to complete one final step. Are you ready to continue?",
"answers": [
{"option": "Yes I am ready", "goto": 5}
]
},
{
"index": 4,
"text": "I understand, I won't force you to help me. But this project is top secret and is only known to a few people, so if you change your mind and decide to help, let me know and we can discuss it further.",
"answers": [
{"option": "Okay, I will keep that in mind", "goto": 6}
]
},
{
"index": 5,
"text": "Fantastic! We just need to make sure that all the safety protocols are in place before we move forward. Let me explain what we need to do next. Are you ready?",
"answers": [
{"option": "Yes, let's do it", "goto": 7}
]
},
{
"index": 6,
"text": "Glad to hear it. I'll be here if you need my help. Take your time and let me know if you decide to help me with this project.",
"answers": []
},
{
"index": 7,
"text": "Fantastic! We just need to make sure that we have all the resources and materials we need before we proceed. Do you want to start working on the project with me?",
"answers": [
{"option": "Yes, let's do it", "goto": 8},
{"option": "No, I want to wait until I'm sure I'm ready", "goto": 6}
]
},
{
"index": 8,
"text": "Great! Let me explain in more detail what we need to do. Are you ready to get started?",
"answers": [
{"option": "Yes, let's do it!", "goto": 9}
]
},
{
"index": 9,
"text": "Fantastic! Let's get to work. We have a lot of work to do, so let's not waste any time. Good luck!"
}
]

# MYSTERY


characters = ["Elizabeth Lancaster", "Michael Lancaster", "Victoria Drake", "Thomas Hayes", "Olivia Thompson", "Samuel Johnson"]
convs = conv_gen6



def conversation(conv):
	index = 0
	while True:
		part = [c for c in conv if c["index"] == index][0]
		print(part["text"])
		if "answers" not in part or len(part["answers"]) == 0:
			break
		for i, ans in enumerate(part["answers"]):
			if "condition" in ans and ans["condition"] not in switches:
				continue
			opt = ans["option"]
			print(f" {i}: {opt}")
		print("CHOOSE: ", end=""),
		chosen = part["answers"][int(input())]
		if "set" in chosen:
			var = chosen["set"]
			print(f"VARIABLE SET: {var}")
			switches.add(chosen["set"])
		index = chosen["goto"]




# while True:
# 	print("\nPick a character:")
# 	for i, char in enumerate(characters):
# 		print(f" {i}: {char}")
# 	print("CHOOSE: ", end="")
# 	char = characters[int(input())]
# 	conversation(convs)
# print("-- THE END --")
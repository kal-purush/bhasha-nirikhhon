'''
# Th1nker model

Why?
Because AI today is not smart enough, for lazy guys like me

What?
Train a model to learn a thinking process steps

How? by :
giving steps in the process
2. splitting knowledge than think process
3. by giving process mechanism to build/access knowledge

-- ideas : rewrite a text in the format "question? answer. question? answer. " and use that text to teach a model to follow instructions with just LLMM standard AR and avoid complex RL

## In details
the thinker can be a transformer base model, made with building blocks responsible for each task in the process

### Blocks
Here are the proposed building blocks:
1. the step process is given by calling during inference:
	1. ReadLayer : read any kind of information
	2. ProcessLayer : process information, always the same
	3. OutputLayer : get output from
	4. Probe (optional) : predicting the block that will be used in the next thinking step
2. processing memory (or short-term memory)
	1. Latent : is as input in all layers mentioned before, it also the initial input of the model
	2. Input : is the input data we want to process, we may have TextInput, ImageInput ... , InputText + Latent -> ReadTextInputLayer
	3. Ouput : is the expected output, optionally autoregressively (output probe)
3. medium-term memory component
	1. LatentStore : we store latent at each processing step, they will be read at inference with ReadLatentLayer
	2. 
4. long-term memory : is also built with latent but of the past run of the model
	1. it will be probably very large and need an indexer to get the useful one
	2. ReadLatentLayer here will be associated with a learned Indexer
	3. 

### Inference


### Training
1. learn how to do step:  
	just train in forward pass-over step with random initial latent,  
	re-read input at each step, to avoid model divergence
	get output at each step a give the loss training signal
	for autoregressive task  
	back-propagrade over many steps, use stop gradient after some step for the model to generalize how to handle/denoise any kind of latent in the middle of the thinking process, we can also simply add noise/mask/...

2. learn how to use memory
	**short memory** represented by the Latent will be learning with the training mentioned before since Latent is the communication mechanism between step
	**medium memory** for this one :
	1. we store latent at each step
	2. we use a ReadLatentLayer to read the previous latent step
	3. backprob signal passing through the read latent, to the layer that produced it. that will give the learning signal to backprop that latent can be re-use, so that backprop can optimize it
	4. keep all those gradients might use a lot of memory
		1. we can stop after a certain number of steps
		2. we will start with a small to study how the number of steps affects the inference 

3. learn how to use/build long-term memory
	for using long-term memory we can keep old latent in a store and read them during inference  
	but since those latent are built to be reused, the learning could be slow  
	to accelerate it we can backdrop the signal like in the medium term memory. that can be challenging to do since the long term it's way larger. An approach is hold it in different GPUs, the required backward state for the backward gradient step, that can only at scale.  

	but we will use a more efficient way to accelerate it. we assume that long term can be read by just reading a lot of input data and keeping them in compressed format in the memory. we can do that by running Input + Latent(compressed signal token) -> ReadInput -> Process -> Latent(compressed)  
	with this approach we can do backprop during training: eg. for text input we can run this on many text sections to get the compressed Latent knowledge and use during step with ReadLatentLayer. the training signal will be backprop thought process, reading and up to the compressed signal token (which a just one token, the other token can be random)  
	we can do this process hierarchically by compressing again the compressed token before giving them to the thinking steps. to avoid losing information, we can make the second step less compressive.  
	**important!** that will also teach the model how to synthesize his own knowledge, via extracting important information from what it already extracted. we might do many times during inference eg. 10 Levels of compression :-) to have contextual compression we can give the compression process the Latent in the current step. Also, that could slow down execution during training by making it more sequential and less parallel.  

4. learn how to index long-term memory  
	indexing because long-term memory, can be very wide - then pre-selected the tokens that will be read by the model will help significantly  
	can be done by learning how to predict the most useful block of memory  
	useful block can be estimated by how much attention is given to them, but might not be meaningful  
	RL can we using considering memory reading block as action, so that the model will how to choose the best one.  

	we can make it easy for the model by caching the most frequently used memory block, maybe they might be general knowledge that are not stored in the language model weight. Like coding skills, some English vocabulary, ... maybe they might differ for a given task such as coding python, or coding java, or writing a novel.  

'''


import math
import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F


class ProcessLayer(nn.Module):
	def __init__(self, d_model, nhead, num_layers=4):
		super().__init__()
		layers = []
		for i in range(num_layers):
			layers.append(nn.TransformerEncoderLayer(d_model, nhead, batch_first=True))
		self.processlayer = nn.Sequential(*layers)

	def forward(self, latent):
		return self.processlayer(latent)

class ReadLayer(nn.Module):
	def __init__(self, vocab_size, d_model, nhead, num_layers=1):
		super().__init__()
		self.embed = nn.Embedding(vocab_size, d_model)
		self.pos_enc = PositionalEncoding(d_model)
		self.readlayer = nn.TransformerDecoderLayer(d_model, nhead, batch_first=True)

	def forward(self, latent, input, read_latent_size):
		input = self.embed(input)
		input = self.pos_enc(input)
		# latent[:, :read_latent_size, :] = self.readlayer(latent[:, :read_latent_size, :], input)
		# todo: check if this code works
		read_latent = self.readlayer(latent[:, :read_latent_size, :], input)
		return torch.cat([latent[:, read_latent_size:, :], read_latent], dim=1)

class ReadLatentLayer(nn.Module):
	def __init__(self, d_model, nhead, num_layers=1):
		super().__init__()
		self.readlayer = nn.TransformerDecoderLayer(d_model, nhead=1, batch_first=True) # todo: check if it perform well with nhead=1
	
	def forward(self, latent, memory, read_latent_size):
		# return self.readlayer(latent, memory)
		read_latent = self.readlayer(latent[:, :read_latent_size, :], memory)
		return torch.cat([latent[:, read_latent_size:, :], read_latent], dim=1)

class OutputLayer(nn.Module):
	def __init__(self, d_model, nhead, vocab_size, num_layers=1):
		super().__init__()
		self.outputlayer = nn.TransformerDecoderLayer(d_model, nhead, tgt_is_causal=True)
		self.proj = nn.Linear(d_model, vocab_size)

	def forward(self, context, latent):
		return self.outputlayer(context, latent)
	
	def get_token(self, token_embed, temperature=1.0, top_k=0, top_p=0.0):
		logit = self.proj(token_embed)
		logit = logit / temperature
		
		# argmax to get the token
		token = torch.argmax(logit, dim=-1)
		return token

class PositionalEncoding(nn.Module):
	def __init__(self, d_model, dropout=0.1, max_len=5000):
		super().__init__()
		self.dropout = nn.Dropout(p=dropout)

		pe = torch.zeros(max_len, d_model)
		position = torch.arange(0, max_len, dtype=torch.float).unsqueeze(1)
		div_term = torch.exp(torch.arange(0, d_model, 2).float() * (-math.log(10000.0) / d_model))
		pe[:, 0::2] = torch.sin(position * div_term)
		pe[:, 1::2] = torch.cos(position * div_term)
		pe = pe.unsqueeze(0).transpose(0, 1)
		self.register_buffer('pe', pe)

	def forward(self, x, step_pos=None):
		if step_pos is None:
			return x + self.pe[step_pos, :]
		x = x + self.pe[:x.size(0), :]
		return self.dropout(x)

# th1nker model
class Thinker(nn.Module):
	def __init__(self, d_model, nhead, vocab_size):
		'''
			instead of unique d_model we may have:
			- d_latent = d_model
			- d_input = d_output
			- d_memory in case latent we have combined latents
		'''
		super().__init__()
		# self.pos_enc = PositionalEncoding(d_model, max_len=200)
		self.processlayer = ProcessLayer(d_model, nhead)
		self.readlayer = ReadLayer(vocab_size, d_model, nhead)
		self.readlatentlayer = ReadLatentLayer(d_model, nhead)
		self.ouputlayer = OutputLayer(d_model, nhead, vocab_size)

		self.d_latent = 256

		self.max_len = 1000
		self.max_steps = 10
		self.max_latent = 256
		self.max_latent_store = self.max_steps * self.max_latent

		self.latent_store = None
		self.permanent_latent_store = torch.randn(1, 1000, 512)
		# self.permanent_latent_store_embed = torch.randn(512)
		# self.medium_memory_latent_store_embed = torch.randn(512)
		self.compress_token_signal_embed = torch.randn(512)
		self.step_count = 0

	def init_latent(self, latent_size, batch_size = None):
		return torch.randn(batch_size, latent_size, self.d_latent)

	def store_latent(self, latent):
		self.latent = latent
		self.step_count += 1
		# add position encoding, same enconding on the whole vector
		# latent = self.pos_enc(latent, step_pos=self.step_count)
		# latent += self.medium_memory_latent_store_embed
		if self.latent_store is None:
			self.latent_store = latent
		self.latent_store = torch.cat((self.latent_store, latent), dim=1)
	
	def init_latent_store(self, long_term_latent=None):
		self.latent_store = long_term_latent

	def forward(self, inputext, split=[128, 0, 4, 18, 8, 23], wide_input=None, latent_size=64):
		batch_size = inputext.shape[0]
		target_chunks = torch.split(inputext, split)
		split = np.cumsum(split)
		input_chunk = inputext[:split[0]]
		latent = self.init_latent(latent_size, batch_size)

		if wide_input:
			compressed_latents = self.forward_compression(wide_input)
			# self.store_latent(compressed_latents)
			B, C, L = compressed_latents.shape # shape = B x C x L
			compressed_latents = compressed_latents.view(B*C,L).repeat(B,1,1) # shape = B x B*C x L
			self.latent_store = torch.cat((self.latent_store, compressed_latents), dim=1)
		latent = self.readlayer(input_chunk, latent)
		latent = self.processlayer(latent)
		self.store_latent(latent)
		
		outputs = []
		for i in range(1, self.max_steps):
			rl = latent.shape[1] // 2 # read latent length, can be randomize
			latent = self.readlayer(input_chunk, latent, rl)
			latent = self.processlayer(latent)
			latent = self.readlatentlayer(latent, self.latent_store, rl)
			latent = self.processlayer(latent)
			self.store_latent(latent)
			
			# output generation
			target_chunk = target_chunks[i+1]
			input_chunk  = torch.cat([input_chunk, target_chunk], dim=1)
			out = self.ouputlayer(target_chunk, latent, tgt_is_causal=True)
			outputs.append(out)

			# this should go in the train loop
			# standart loss computation
			out_tokens = self.ouputlayer.get_token(out)
			loss = CategoricalCrossEntropyLoss(out_tokens, target_chunk)
			# loss with knowledge distillation from parent logit
			out_logits = self.ouputlayer.proj(out)
			parents_logits = inputext.parents_logits[i+1]
			loss = mse(out_logits, parents_logits)
		return outputs
		

	def foward_step(self, inputext, latent):
		x = inputext
		x = self.embed(x)
		x = self.pos_enc(x)

		latent = self.readlayer(x, latent)
		latent = self.processlayer(latent)
		latent = self.readlatentlayer(latent, self.latent_store)
		latent = self.processlayer(latent)
		self.store_latent(latent)

		return latent


	def forward_output(self, latent, max_token):
		# auto-regressive generation loop
		out_embed = torch.zeros(1, 1, 512) # start token
		out_token = torch.zeros(1, 1, dtype=torch.long)
		for i in range(max_token):
			out_embed = self.pos_enc(out_embed)
			out_embed = self.ouputlayer(out_embed, latent, tgt_is_causal=True)
			out_token = self.ouputlayer.get_token(out_embed)
			out_embed = self.embed(out_token)
			if out_token == 1: # end token
				break

		return out_token, latent
	
	def forward_compression(self, inputext, latent_size):
		batch_size, chunks, lenght = inputext.shape
		latent = self.init_latent(latent_size)
		latent = torch.cat([self.compress_token_signal_embed, latent], dim=1)

		latents = self.readlayer(inputext, latent)
		latents = self.processlayer(latents)
		latents = self.readlayer(inputext, latents)
		latents = self.processlayer(latents)

		return latents



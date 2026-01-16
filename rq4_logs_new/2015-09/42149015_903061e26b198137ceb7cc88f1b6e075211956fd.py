import numpy as np
#Busca gulosa para um caca palavras, NxN, horizontal, vertical, diagonal, normal ou invertido
def caca_palavras(palavras, tabela):
	#Recebe uma tabela NxN (matriz) e as palavras para serem escritas na tabela (array)
	tabela = np.array(tabela)
	tabela = np.insert(tabela, len(tabela)+1, ' ', axis=1)
	tabela = np.insert(tabela, 0, ' ', axis=0)
	tabela = np.insert(tabela, 0, ' ', axis=1)
	tabela = np.insert(tabela, len(tabela), ' ', axis=0)
	print tabela
	resu = np.array([["" , 0, 0], ["" , 0, 0]])
	for pal in palavras:
		a = busca_normal(pal.upper(), tabela)
		if a == None:
			print "A palavra '"+pal+"' nao foi encontrada."
		else:
			resu = np.insert(resu, len(resu), a, axis = 0)

	#t = len(resu)
	#resu = resu[4:]
	#resu = resu.reshape(t/3, t))
	if a != None:
		print ("As palavras encontradas foram:")
	return resu[2:]

def buscaN(cam, pal, tabela, prox):
	#se a palavra for nula retorna o caminho
	if pal == "":
		return np.insert(cam, len(cam), ["-", "-", "-"], axis = 0)
	else:
		#x, y - posicoes 
		x = prox[0]
		y = prox[1]
		#var - proximo valor
		val = tabela[x, y]
		if val == pal[0]:
			cam = np.insert(cam, len(cam), [val, str(x), str(y)], axis = 0)
			return buscaN(cam, pal[1:len(pal)], tabela, [x+1, y])
def buscaNE(cam, pal, tabela, prox):
	#se a palavra for nula retorna o caminho
	if pal == "":
		return np.insert(cam, len(cam), ["-", "-", "-"], axis = 0)
	else:
		#x, y - posicoes 
		x = prox[0]
		y = prox[1]
		#var - proximo valor
		val = tabela[x, y]
		if val == pal[0]:
			cam = np.insert(cam, len(cam), [val, str(x), str(y)], axis = 0)
			return buscaNE(cam, pal[1:len(pal)], tabela, [x+1, y+1])
def buscaE(cam, pal, tabela, prox):
	#se a palavra for nula retorna o caminho
	if pal == "":
		return np.insert(cam, len(cam), ["-", "-", "-"], axis = 0)
	else:
		#x, y - posicoes 
		x = prox[0]
		y = prox[1]
		#var - proximo valor
		val = tabela[x, y]
		if val == pal[0]:
			cam = np.insert(cam, len(cam), [val, str(x), str(y)], axis = 0)
			return buscaE(cam, pal[1:len(pal)], tabela, [x, y+1])
def buscaSE(cam, pal, tabela, prox):
	#se a palavra for nula retorna o caminho
	if pal == "":
		return np.insert(cam, len(cam), ["-", "-", "-"], axis = 0)
	else:
		#x, y - posicoes 
		x = prox[0]
		y = prox[1]
		#var - proximo valor
		val = tabela[x, y]
		if val == pal[0]:
			cam = np.insert(cam, len(cam), [val, str(x), str(y)], axis = 0)
			return buscaSE(cam, pal[1:len(pal)], tabela, [x-1, y+1])
def buscaS(cam, pal, tabela, prox):
	#se a palavra for nula retorna o caminho
	if pal == "":
		return np.insert(cam, len(cam), ["-", "-", "-"], axis = 0)
	else:
		#x, y - posicoes 
		x = prox[0]
		y = prox[1]
		#var - proximo valor
		val = tabela[x, y]
		if val == pal[0]:
			cam = np.insert(cam, len(cam), [val, str(x), str(y)], axis = 0)
			return buscaS(cam, pal[1:len(pal)], tabela, [x-1, y])
def buscaSO(cam, pal, tabela, prox):
	#se a palavra for nula retorna o caminho
	if pal == "":
		return np.insert(cam, len(cam), ["-", "-", "-"], axis = 0)
	else:
		#x, y - posicoes 
		x = prox[0]
		y = prox[1]
		#var - proximo valor
		val = tabela[x, y]
		if val == pal[0]:
			cam = np.insert(cam, len(cam), [val, str(x), str(y)], axis = 0)
			return buscaSO(cam, pal[1:len(pal)], tabela, [x-1, y-1])
def buscaO(cam, pal, tabela, prox):
	#se a palavra for nula retorna o caminho
	if pal == "":
		return np.insert(cam, len(cam), ["-", "-", "-"], axis = 0)
	else:
		#x, y - posicoes 
		x = prox[0]
		y = prox[1]
		#var - proximo valor
		val = tabela[x, y]
		if val == pal[0]:
			cam = np.insert(cam, len(cam), [val, str(x), str(y)], axis = 0)
			return buscaO(cam, pal[1:len(pal)], tabela, [x, y-1])
def buscaNO(cam, pal, tabela, prox):
	#se a palavra for nula retorna o caminho
	if pal == "":
		return np.insert(cam, len(cam), ["-", "-", "-"], axis = 0)
	else:
		#x, y - posicoes 
		x = prox[0]
		y = prox[1]
		#var - proximo valor
		val = tabela[x, y]
		if val == pal[0]:
			cam = np.insert(cam, len(cam), [val, str(x), str(y)], axis = 0)
			return buscaNO(cam, pal[1:len(pal)], tabela, [x+1, y-1])

def busca_normal(pal, tabela):
	x = np.shape(tabela)[0]
	y = np.shape(tabela)[1]
	t = np.array([["" , 0, 0]])
	for i in range(x):
		for j in range(y):
			cam = buscaN(t, pal, tabela, [i, j])
			if cam != None:
				return cam[1:,]
			cam = buscaNE(t, pal, tabela, [i, j])
			if cam != None:
				return cam[1:,]
			cam = buscaE(t, pal, tabela, [i, j])
			if cam != None:
				return cam[1:,]
			cam = buscaSE(t, pal, tabela, [i, j])
			if cam != None:
				return cam[1:,]
			cam = buscaS(t, pal, tabela, [i, j])
			if cam != None:
				return cam[1:,]
			cam = buscaSO(t, pal, tabela, [i, j])
			if cam != None:
				return cam[1:,]
			cam = buscaO(t, pal, tabela, [i, j])
			if cam != None:
				return cam[1:,]
			cam = buscaNO(t, pal, tabela, [i, j])
			if cam != None:
				return cam[1:,]


def busca_normal2(pal, tabela):
	x = np.shape(tabela)[0]
	y = np.shape(tabela)[1]
	t = np.array([["" , 0, 0]])
	cam = []
	for i in range(x):
		for j in range(y):
			cam = buscaN(t, pal, tabela, [i, j])
			if cam != None:
				cam +=  [cam[1:,]]
			cam = buscaNE(t, pal, tabela, [i, j])
			if cam != None:
				cam +=  [cam[1:,]]
			cam = buscaE(t, pal, tabela, [i, j])
			if cam != None:
				cam +=  [cam[1:,]]
			cam = buscaSE(t, pal, tabela, [i, j])
			if cam != None:
				cam +=  [cam[1:,]]
			cam = buscaS(t, pal, tabela, [i, j])
			if cam != None:
				cam +=  [cam[1:,]]
			cam = buscaSO(t, pal, tabela, [i, j])
			if cam != None:
				cam +=  [cam[1:,]]
			cam = buscaO(t, pal, tabela, [i, j])
			if cam != None:
				cam +=  [cam[1:,]]
			cam = buscaNO(t, pal, tabela, [i, j])
			if cam != None:
				cam +=  [cam[1:,]]
	return cam


tabela = [
["N","O","I","T","E","R","X","P"],
["P","S","V","E","N","T","O","T"],
["A","W","Y","P","Y","R","W","S"],
["B","A","I","V","W","D","I","A"],
["O","O","X","C","A","R","C","O"],
["A","A","I","E","N","T","O","T"],
["A","A","A","A","A","A","U","L"]
]

palavras = ["boi", "circo", "dia", "lua", "noite", "vento"]

print caca_palavras(palavras, tabela)
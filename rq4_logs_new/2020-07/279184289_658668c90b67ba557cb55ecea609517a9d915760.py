from Q_left_right import Q_graph

if __name__ == '__main__':
		q_graph_dict = dict()
		for i in range(5):
				for j in range(5):
						q_graph_dict[(i,j)] = {(i-1, j): (1,0), (i+1, j): (1,0), (i, j-1): (1,0), (i, j+1): (1,0)}

		for i in range(5):
				q_graph_dict[(i,0)].pop((i,-1), None)
				q_graph_dict[(i,4)].pop((i,5), None)
				q_graph_dict[(0,i)].pop((-1,i), None)
				q_graph_dict[(4,i)].pop((5,i), None)

		q_graph_dict[(3,4)][(4,4)] = (1, 1000)
		q_graph_dict[(4,3)][(4,4)] = (1, 1000)
		q_graph_dict[(4,4)] = {}

		print(q_graph_dict)


		q_graph = Q_graph(q_graph_dict, gamma = 0.1)
		q_graph.optimize(iterations=1000,supidity_rate=.1)
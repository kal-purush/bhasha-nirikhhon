import numpy as np

def calc_penalties(data, vectors, index):
    """
    
    """
    vec = vectors[:, index]
    penalties = np.zeros_like(np.dot(data, vectors[:, 0]))
    
    for i in range(index):
        result = np.dot(data, vectors[:, i])
        penalties += (np.dot(np.dot(data, vec), result) /
         np.dot(result, result)
        ) * result

    return penalties 
     
def eigengame(data, n, epochs=100, learning_rate=0.1):
    """
        
    """
    
    M = np.dot(data.T, data)
    dim = M.shape[0]
    vectors = np.ones((dim, n))
    for t in range(n):
        print("Starting vector ", t)
        for epoch in range(epochs):
            rewards = np.dot(data, vectors[:, t])
            penalties = calc_penalties(data, vectors, t)
            
            delta_v = 2*np.dot(data.T, rewards - penalties)
            vectors[:, t] = vectors[:, t] + learning_rate * delta_v
            vectors[:, t] = vectors[:, t] / np.linalg.norm(vectors[:, t])
            
    return M, vectors.T
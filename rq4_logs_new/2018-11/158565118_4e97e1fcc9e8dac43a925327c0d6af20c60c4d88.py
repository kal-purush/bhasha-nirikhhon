import tensorflow as tf
import numpy as np

def cross_entropy_loss(inputs, true_w):
    """
    ==========================================================================

    inputs: The embeddings for context words. Dimension is [batch_size, embedding_size].
    true_w: The embeddings for predicting words. Dimension of true_w is [batch_size, embedding_size].

    Write the code that calculate A = log(exp({u_o}^T v_c))

    A =


    And write the code that calculate B = log(\sum{exp({u_w}^T v_c)})


    B =

    ==========================================================================
    """
    uo = true_w
    vc = inputs
    A = tf.matmul(vc, tf.transpose(uo))
    A = tf.diag_part(A)
    # print (vc.get_shape(), uo.get_shape())
    B = tf.log( tf.reduce_sum( tf.exp( tf.matmul(vc, tf.transpose(uo)) ), 1) )
    # print (A.get_shape(), B.get_shape())
    return tf.subtract(B, A)

def nce_loss(inputs, weights, biases, labels, sample, unigram_prob):
    """
    ==========================================================================

    inputs: Embeddings for context words. Dimension is [batch_size, embedding_size].
    weigths: Weights for nce loss. Dimension is [Vocabulary, embedding_size].
    biases: Biases for nce loss. Dimension is [Vocabulary, 1].
    labels: Word_ids for predicting words. Dimesion is [batch_size, 1].
    sample: Word_ids for negative samples. Dimension is [num_sampled].
    unigram_prob: Unigram probability. Dimesion is [Vocabulary].

    Implement Noise Contrastive Estimation Loss Here

    ==========================================================================
    """

    sample_size = len(sample)
    input_size = inputs.get_shape().as_list()
    batch_size = input_size[0]
    embedding_size = input_size[1]
    k = sample_size

    sample = tf.convert_to_tensor(sample)
    
    uc = inputs
    uc = tf.cast(uc, tf.float64)
    uc_transpose=tf.transpose(uc)

    uo = tf.gather(weights, labels) # labels: output words that are paired with each context words // word ids
    uo = tf.cast(uo, tf.float64)
    uo = tf.reshape(uo, [batch_size, embedding_size])


    vk = tf.gather(weights, sample) #negative samples representation
    vk = tf.reshape(vk, [sample_size, embedding_size])
    vk = tf.cast(vk, tf.float64)

    unigram_prob_tensor=tf.convert_to_tensor(unigram_prob, dtype=tf.float64)


    # print("1", k , uo, uc, vk)
    # up = tf.nn.embedding_lookup(unigram_prob, labels)
    #First Term
    # uo = tf.Print(uo, [uo], message='DEBUG uo: ')

    mult_uc_t_uo = tf.diag_part(tf.matmul(uo, uc_transpose))    
    mult_uc_t_uo = tf.reshape(mult_uc_t_uo, [batch_size, 1])
    # print(mult_uc_t_uo)
    # mult_uc_t_uo = tf.Print(mult_uc_t_uo, [mult_uc_t_uo], message='DEBUG mult_uc_t_uo: ')
    pos_bias = tf.gather(biases,labels) # uo bias
    pos_bias = tf.cast(pos_bias, tf.float64)
    # pos_bias = tf.Print(pos_bias, [pos_bias], message='DEBUG pos_bias: ')
    # print(pos_bias)
    s_uo_uc = tf.add(mult_uc_t_uo , pos_bias)
    # s_uo_uc = tf.Print(s_uo_uc, [s_uo_uc], message='DEBUG s_uo_uc: ')
    # print(s_uo_uc)
    uo_p = tf.gather(unigram_prob_tensor,labels)
    uo_p = tf.reshape(uo_p, [batch_size, -1])
    # print(uo_p.get_shape())
    first_term = tf.subtract(s_uo_uc, tf.log( tf.scalar_mul(float(k),uo_p) ) )
    # print(first_term.get_shape())



    # vk_transpose=tf.transpose(vk)
    # uo_p = tf.Print(uo_p, [uo_p], message='DEBUG uo_p: ')
    # first_term = tf.Print(first_term, [first_term], message='DEBUG first_term: ')

    # vk = tf.Print(vk, [vk], message='DEBUG vk: ')
    #Second Term

    # print(vk.get_shape(), uc_transpose.get_shape())
    mult_uc_t_vk = tf.matmul(vk, uc_transpose )
    # print(mult_uc_t_vk.get_shape())
    neg_bias=tf.gather(biases,sample) # vk bias
    neg_bias = tf.reshape(neg_bias, [sample_size, -1])
    neg_bias = tf.cast(neg_bias, tf.float64)
    s_uc_vk = tf.add(mult_uc_t_vk , neg_bias)
    vk_p = tf.gather(unigram_prob_tensor,tf.transpose(sample))
    vk_p = tf.reshape(vk_p, [k, -1])
    # print(neg_bias.get_shape(), s_uc_vk.get_shape(), vk_p.get_shape())
    # vk_p = tf.gather(unigram_prob_tensor,tf.transpose(sample))
    second_term = tf.subtract(s_uc_vk, tf.log( tf.scalar_mul(float(k),vk_p) ) )
    # print(second_term.get_shape())


    # mult_uc_t_uo = tf.Print(mult_uc_t_uo, [mult_uc_t_uo], message='DEBUG mult_uc_t_uo: ')
    # mult_uc_t_vk = tf.Print(mult_uc_t_vk, [mult_uc_t_vk], message='DEBUG mult_uc_t_vk: ')

    # print("2",mult_uc_t_uo, mult_uc_t_vk)


    # neg_bias = tf.reshape(neg_bias, [-1, embedding_size])
    # uo = tf.reshape(uo, [-1, embedding_size])

    # pos_bias = tf.reshape(pos_bias, [-1])

    # print("3",pos_bias, neg_bias)


    # print("4", s_uo_uc, s_uo_vk)
    # s_uc_vk = tf.Print(s_uc_vk, [s_uc_vk], message='DEBUG s_uc_vk: ')



    # print("5", uo_p, vk_p)
    # uo_p = tf.Print(uo_p, [uo_p], message='DEBUG uo_p: ')
    # vk_p = tf.Print(vk_p, [vk_p], message='DEBUG vk_p: ')

    # first_term =tf.subtract(U_score, tf.log(tf.scalar_mul(k,P_o)+1e-10))
    # second_term = tf.subtract(N_score,tf.log(tf.scalar_mul(k,P_n)+1e-10))

    # print("6",first_term, second_term)
    # print("Value", first_term[0].value, second_term[0][0].value)



    # sigmoid_2 = tf.sigmoid(second_term)
    # print("7", sigmoid_1, sigmoid_2)
    # sigmoid_1 = tf.Print(sigmoid_1, [sigmoid_1], message='DEBUG sigmoid_1: ') 
    # sigmoid_2 = tf.Print(sigmoid_2, [sigmoid_2], message='DEBUG sigmoid_2: ')
    # print("value" , sigmoid_1[0].value,sigmoid_2[0][0].value)
    # print("8", I)

    sigmoid_1 = tf.sigmoid(first_term)
    I = tf.constant(1.0,shape = [k, 1], dtype= tf.float64)
    A = tf.log(sigmoid_1)
    B = tf.reduce_sum(tf.scalar_mul(-1.0, tf.log(tf.add(I, tf.exp(second_term)))), 0)
    return tf.scalar_mul(-1.0,tf.add(A,B))




def nce_loss1(inputs, weights, biases, labels, sample, unigram_prob):
    """
    ==========================================================================
    inputs: Embeddings for context words. Dimension is [batch_size, embedding_size].
    weigths: Weights for nce loss. Dimension is [Vocabulary, embeeding_size].
    biases: Biases for nce loss. Dimension is [Vocabulary, 1].
    labels: Word_ids for predicting words. Dimesion is [batch_size, 1].
    samples: Word_ids for negative samples. Dimension is [num_sampled].
    unigram_prob: Unigram probability. Dimesion is [Vocabulary].
    Implement Noise Contrastive Estimation Loss Here
    ==========================================================================
    """
    reshaped_labels = tf.reshape(labels,[-1])
    
    #u_o = tf.gather(weights,reshaped_labels,axis=0)
    u_o = tf.gather(weights,reshaped_labels,axis=0)
    #print (tf.gather(weights,labels,axis=0).get_shape())
    # print (u_o.get_shape())
    
    tbias = tf.gather(biases,reshaped_labels,axis=0)
    u_o_biases = tf.reshape(tbias,[-1])
    u_o_probs = tf.gather(unigram_prob,reshaped_labels,axis=0)
    # print (u_o_probs.get_shape())
    
    # u_o = tf.Print(u_o, [u_o], message='DEBUG u_o: ')
    # tbias = tf.Print(tbias, [tbias], message='DEBUG tbias: ')
    # u_o_biases = tf.Print(u_o_biases, [u_o_biases], message='DEBUG u_o_biases: ')
    # u_o_probs = tf.Print(u_o_probs, [u_o_probs], message='DEBUG u_o_probs: ')

    n_t_e = tf.gather(weights,sample,axis=0)
    # print (n_t_e.get_shape())
    
    
    n_t_e_probs = tf.gather(unigram_prob,np.array([sample]).T,axis=0)
    # print (n_t_e_probs.get_shape())
    
    n_t_e_biases = tf.reshape(tf.reshape(tf.gather(biases,sample,axis=0),[-1]),[(sample.size),1])
    # print (n_t_e_biases.get_shape())

    
    u_c = tf.cast(inputs,tf.float64)
    u_o = tf.cast(u_o,tf.float64)
    u_o_biases = tf.cast(u_o_biases,tf.float64)
    u_o_probs = tf.cast(u_o_probs,tf.float64)
    
    #start equation calculation
    swow1 = tf.add(tf.diag_part(tf.matmul(u_o, tf.transpose(u_c))),u_o_biases)
    # swow1 = tf.Print(swow1, [swow1], message='DEBUG biase + mul: ')

    insideSigmoid = tf.subtract(swow1,tf.log(float(sample.size)*u_o_probs))
    # insideSigmoid = tf.Print(insideSigmoid, [insideSigmoid], message='DEBUG first term: ')

    one_mat = tf.cast(tf.constant(1.0),dtype=tf.float64)
    part1 = -tf.log(tf.add(one_mat,tf.exp(-insideSigmoid)))
    #part1 = -(tf.nn.softplus(-insideSigmoid))
    #part1 = tf.Print(part1,[part1],message = 'check value')
    #print (part1)  
    
    n_t_e = tf.cast(n_t_e,tf.float64)
    
    n_t_e_biases = tf.cast(n_t_e_biases,tf.float64)
    n_t_e_probs = tf.cast(n_t_e_probs,tf.float64)
    
    swxwc = tf.add(tf.matmul(n_t_e, tf.transpose(u_c)),n_t_e_biases)
    # print (swxwc.get_shape())
    # print (tf.matmul(n_t_e, tf.transpose(u_c)).get_shape(),n_t_e_biases.get_shape(),'check dimension')
    #print('reached here')
    Pr = tf.subtract(swxwc,tf.log(float(sample.size)*n_t_e_probs))
    #print('reached here')
    #log(1-1/(1+e^(-x))) = log(1/(1+e^(x)) = -log(1+e^(x)) = -log(Pr))
    deno = tf.exp(Pr)
    #print('reached here')
    one_mat = tf.cast(tf.constant(1.0),dtype=tf.float64)
    logOfdeno = tf.log(tf.add(one_mat,deno))
    
    part2 = tf.reduce_sum(logOfdeno,axis=0)
    # print (part2.get_shape())  
    #print ((tf.subtract(part1,part2)).get_shape())
    return -tf.subtract(part1,part2)

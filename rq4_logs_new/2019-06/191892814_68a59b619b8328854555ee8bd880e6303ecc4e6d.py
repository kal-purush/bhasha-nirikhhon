def get_params_for_act(demand, id_distr, params_array):

    # This function retrieves all exchanges for an activity for both bio and tech arrays
    # params_array is either lca.tech_params or lca.bio_params
    
    mask= params_array["col"] == lca.activity_dict[demand]
    mask_distr = params_array["uncertainty_type"][mask] == id_distr
    indices = list(compress(range(len(mask_distr)), mask_distr))
    params = params_array[indices_tech]
        
    return indices, params

distr_id = {
        "lognorm": 2,
        "norm": 3,
        "uniform": 4,
        "triang": 5
        }

def params_dict(demand, distr_id):
    
    # This function "should" return p as in the structure of params explained below
    
    p = {}
    array_type = {"tech": lca.tech_params,
                  "bio": lca.bio_params}
    for type_ in array_type:
        for distr, id_ in distr_id:
            p[type_][distr]=get_params_act(demand, distr_id(distr), array_type[type_])
    
    return p

def params_replace (array, sobol_sample, params_type, indices_type):

    # This function replace in array (which can be tech_params or bio_params) new values based on sobol sample according to distributions
    # params and indices should be in the form of:
    # params = {
    #        tech: {
    #                norm: ....,
    #                triang: ....,
    #                uniform: ....,
    #                lognorm: ....,
    #                }
    #        bio: {
    #                norm: ....,
    #                triang: ....,
    #                uniform: ....,
    #                lognorm: ....,
    #                }
    #        }
    # params_type is params["tech"] or params["bio"] and the same goes for indices_type

    vector_amount = array["amount"]
    counter = 0
    
    # Normal distribution
    q = (q_high-q_low)*sobol_sample[:len(indices_type["norm"])] + q_low
    params_norm_new  = norm.ppf(q,loc=params_type["norm"]['loc'],scale=params_type["norm"]['scale'])
    np.put(vector_amount, indices_type["norm"], params_norm_new)
    del q
    del sobol_sample[:len(indices_type["norm"])] # This is to make it easier to select next columns
    
    # Triangular distribution
    q = sobol_sample[:len(indices_type["triang"])]
    loc   = params_type["triang"]['minimum']
    scale = params_type["triang"]['maximum']-params_type["triang"]['minimum']
    c     = (params_type["triang"]['loc']-loc)/scale
    params_triang_new = triang.ppf(q,c=c,loc=loc,scale=scale)
    np.put(vector_amount, indices_type["triang"], params_triang_new)
    del q
    del sobol_sample[:len(indices_type["triang"])]
    
    # Uniform distribution
    params_uniform_new = (sobol_sample[:len(indices_type["uniform"])]
                            * (params_type["uniform"]["maximum"] - params_type["uniform"]["minimum"])) 
                            + params_type["uniform"]["minimum"]
    np.put(vector_amount, indices_type["unifom"], params_uniform_new)
    del sobol_sample[:len(indices_type["uniform"])]
    
    # Lognor distribution
    q = (q_high-q_low)*samples[:len(indices_type["uniform"])] + q_low
    params_lognorm_new = lognorm.ppf(q,s=params_type["lognorm"]['scale'],scale=np.exp(params_type["lognorm"]['loc']))
    np.put(vector_amount, indices_type["lognorm"], params_lognorm_new)
    
    ### Need to include code for discrete_uniform distribution
    return vector_amount
    # Question, does this need to return something?
    
    
def score_from_sample(lca, sobol_sample, params):
    ## This function calculates new scores 
    
    tech_vector = params_replace(lca.tech_params, sobol_sample, params["tech"], indices["tech"])
    bio_vector = params_replace(lca.bio_params, sobol_sample, params["bio"], indices["bio"])
    
    lca.rebuild_biosphere_matrix(bio_vector):
    lca.rebuild_technosphere_matrix(tech_vector)
    
    c = sum(lca.characterization_matrix)
    score = ((c*lca.biosphere_matrix))*spsolve(lca.technosphere_matrix,d))[0]
    
    return score
from rfim2d import residuals

r_list = [1., 2., 3.]
Sigma_list = [1., 1., 1.]
eta_list = [1., 1., 1.]

def test_Sigma_residual():

    func_type = 'power law'
    keys = ['rScale', 'rc', 'sScale', 'sigma']
    fixed_dict = dict([('rScale', 1.)])
    args = [r_list, Sigma_list, keys, fixed_dict, func_type]
    params = [0., 1., 1.]
    residual = residuals.Sigma_residual(params, args)
    print(residual)

    func_type = 'well-behaved'
    keys = ['rScale', 'rc', 'sScale', 'df', 'B', 'C']
    fixed_dict = dict([('df', 2.), ('C', 0.)])
    args = [r_list, Sigma_list, keys, fixed_dict, func_type]
    params = [1., 1., 1., 1.]
    residual = residuals.Sigma_residual(params, args)
    print(residual)

    return


def test_eta_residual():

    func_type = 'power law'
    keys = ['rScale', 'rc', 'etaScale', 'betaDelta']
    fixed_dict = dict([('etaScale', 1.)])
    args = [r_list, eta_list, keys, fixed_dict, func_type]
    params = [1., 1., 1.]
    residual = residuals.eta_residual(params, args)
    print(residual)

    func_type = 'well-behaved'
    keys = ['rScale', 'rc', 'etaScale', 'lambdaH', 'B', 'F']
    fixed_dict = None
    args = [r_list, eta_list, keys, fixed_dict, func_type]
    params = [1., 1., 1., 1., 1., 1.]
    residual = residuals.eta_residual(params, args)
    print(residual)

    return

#def test_joint_residual():
#
#    func_type = 'power law'
#    args = [r_list, Sigma_list, r_list, eta_list, func_type]
#    param_dict = dict([('rScale',1.), ('rc', 0.), ('sScale', 1.), ('etaScale', 1.), ('sigma', 1.), ('betaDelta', 1.)])
#    residual = residuals.joint_residual(param_dict, args)
#    print(residual)
#
#    func_type = 'well-behaved'
#    args = [r_list, Sigma_list, r_list, eta_list, func_type]
#    param_dict = dict([('rScale',1.), ('rc', 0.), ('sScale', 1.), ('etaScale', 1.), ('df', 2.), ('lambdaH', 1.), ('B', 1.), ('C', 1.), ('F', 1.)])
#    residual = residuals.joint_residual(param_dict, args)
#    print(residual)
#
#    return

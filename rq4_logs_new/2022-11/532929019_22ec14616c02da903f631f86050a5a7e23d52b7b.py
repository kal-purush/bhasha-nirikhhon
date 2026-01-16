import math
import numpy as np
import pandas as pd
import scipy.stats.qmc as qmc
import cvxpy as cp  # later: without this package, with linear programming

from estimagic.estimation.msm_weighting import get_weighting_matrix
from estimagic.estimation.estimate_msm import get_msm_optimization_functions


def check_msm_identification(
    simulate_moments,
    msm_res,
    moments_cov,
    draws,
    n_obs,
    weights="diagonal",
    kernel="uniform",
    sampling="sobol",
    bandwidth=None,
    cutoff=None,
    population_mc_kwgs=None,
    simulate_moments_kwargs=None,
    logging=False,
    log_options=None,
):
    """Detection of the identification failure in moment condition models.
    Performs the identification check as described in Forneron, J. J. (2019).
    Introduces the quasi-Jacobian matrix computed as a slope of a linear
    approximation of the moments of the estimate of the identified set. It is
    asymptotically singular when local or global identification fails and equivalent
    to the usual Jacobian matrix which has full rank when model is point and locally
    identified.

    Args
        simulate_moments (callable) – Function that takes as inputs model parameters,
            data and potentially other keyword arguments and returns a pytree with
            simulated moments.  If the function returns a dict containing the key
            "simulated_moments" we only use the value corresponding to that key. Other
            entries are stored in the log database if you use logging.
        simulate_moments_kwargs (dict) – Additional keyword arguments for simulate_moments
            with, for example, data on dependent and independent variables from the model
            specification.
        msm_res (dict) – The output of ``estimate_msm`` including estimated
        parameters and standard errors.
        weights (str) – One of “diagonal” (default), “identity” or “optimal”.  Note that
            “optimal” refers to the asymptotically optimal weighting matrix and is often
            not a good choice due to large finite sample bias.
        bandwidth (float) - By default is calculated in the form of sqrt(2log(log[n])/n).
            Required for the selection of subsets for levels sets.
        kernel (callable) - By default  is the uniform kernel K(U) which is indicator
            function for |U|<=1. Required for the calculation of quasi-jacobian matrix.
        cutoff (float) - By default is calculated in the form sqrt(2log[n]/n). Required
            for identification category selection.
        draws (float)  - The number of draws for sampling on level sets. Supposed to be
            sufficiently large.
        n_obs (int): Number of observations. Required for calculation of bandwidth and cutoff.
        sampling (str) - Methods of sampling uniformly from likelihood level sets. One of
            the available options for direct approach using "sobol" or "halton" sequence
            or adaptive sampling by "population_mc".
        population_mc_kwgs (dict): Further tuning parameters for adaptive sampling.
        significance (float) - The significance level with default level 5%.
        H0 (dict) - Required for subvector inference. For example, b10 = 0.
        logging (pathlib.Path, str or False) – Path to sqlite3 file (which typically has
            the file extension .db. If the file does not exist, it will be created. The
            dashboard can only be used when logging is used.
        log_options (dict) – Additional keyword arguments to configure the logging.

    Returns
        dict: The estimated quasi-Jacobian, singular values, identification category."""
    # params (pytree) – A pytree containing the estimated parameters of the model.
    #        Pytrees can be a numpy array, a pandas Series, a DataFrame with “value” column, a
    #        float and any kind of (nested) dictionary or list containing these elements.

    # check whether the inputs are valid
    if sum(msm_res.summary()["standard_error"].isnull()) > 0:
        raise ValueError(
            "Standard error is NA for some of the estimated parameters. Cannot proceed."
        )

    msm_params = msm_res.params["value"]  # estimated coefficient vector
    n_params = len(msm_params)  # number of estimated parameters

    # calculate default inputs
    if bandwidth is None:
        bandwidth = math.sqrt(2 * math.log(math.log(n_obs)) / n_obs)  # for step 2.i)
    if cutoff is None:
        cutoff = math.sqrt(2 * math.log(n_obs) / n_obs)  # for step 3.ii)

    # (in the paper: Step 2.i Draw Uniformly on the Level Set)
    # 1. quasi-Jacobian Matrix
    # 1.1 set the integration grid and evaluate the moments on the grid,
    # select draws on the level set
    grid_sub, moms_sub = sampling_level_sets(
        simulate_moments, msm_res, moments_cov, draws, bandwidth, weights, sampling
    )

    if len(grid_sub) == 0:
        raise ValueError("No draws fall in a level subset. Cannot proceed.")

    # (in the paper: Step 2.ii-iii) Linear Approximation and quasi-Jacobian; Compute variance)
    # 1.2 compute the intercept and slope
    # 1.3 compute the variance
    Bn, phi = calculate_quasi_jacobian(grid_sub, moms_sub, n_params)

    # (in the paper: Step 3 Identification Category Selection)
    # 2. Identification Category Selection
    # 2.1 compute singular values
    # 2.2 number of values grater than cutoff
    sing, cutoff, n_sing = category_selection(moments_cov, n_params, Bn, phi, cutoff)

    # 3. Subvector Inference
    # 3.1 test statistic
    # 3.2 hypothesis, confidence set

    return Bn, sing, cutoff, n_sing


def sampling_level_sets(
    simulate_moments,
    msm_res,
    moments_cov,
    draws,
    bandwidth,
    weights,
    sampling,
    population_mc_kwgs=None,
):
    """Calculates the uniform draws over the level set required for the computation
    of the quasi-Jacobean. Uses either direct approach with random/pseudo-random
    sequences (Sobol or Halton) or adaptive sampling by Population Monte Carlo.
    For the direct approach the effective sample size tends to be too small when
    the dimension of the parameter is moderately large. Adaptive sampling constructs
    a sequence of proposal distributions with higher acceptance rate.

    Args:
        simulate_moments
        msm_res (dict) – The output of ``estimate_msm`` including estimated
        parameters and standard errors.
        moments_cov
        draws (float) - Number of initial draws for sampling.
        bandwidth (float) - By default is calculated in the form of sqrt(2log(log[n])/n).
            Required for the selection of subsets for levels sets.
        weights
        sampling (str) - Methods of sampling uniformly from likelihood level sets.
            One of the available options for direct approach using "random",
            "sobol" or "halton" sequence or adaptive sampling by "population_mc".
        population_mc_kwgs (dict): Further tuning parameters for adaptive sampling.

    Returns:
        The selected draws, simulated moments and variance.

    """
    msm_params = msm_res.params["value"]
    msm_se = msm_res.summary()["standard_error"]
    n_params = len(msm_params)

    if sampling not in ["random", "sobol", "halton", "population_mc"]:
        raise NotImplementedError("Custom sampling is not yet implemented.")

    # 1. DRAWS
    # 1.1 DIRECT APPROACH
    if sampling in ["random", "sobol", "halton"]:
        if sampling == "random":
            sequences = np.random.random((draws, n_params))

        elif sampling == "sobol":
            sequences = qmc.Sobol(
                d=n_params, scramble=True
            )  # d - dimension - number of estimated parameters
            sequences = sequences.random(
                n=draws
            )  # instead of n=B try power of 2 (for Sobol sequence)

        else:
            raise NotImplementedError(
                "Sampling with Halton sequences is not yet implemented."
            )

        params_draws = np.array([list(msm_params)] * draws) + [
            list(3 * msm_se * (sequences[i] - 1 / 2)) for i in range(draws)
        ]  # [list(msm_params)]*draws + 2 * (sequences - 1 / 2)

    # 1.2. ADAPTIVE SAMPLING - TO DO
    elif sampling == "population_mc":
        raise NotImplementedError(
            "Adaptive Sampling by Population Monte Carlo is not yet implemented."
        )

    else:
        raise NotImplementedError("Custom sequences are not yet implemented.")

    # 2. WEIGHTING THE RESULTED DRAWS
    objs = [np.nan] * draws  # Store MSM objective values
    moms = [[np.nan] * n_params] * draws  # Store sample moments mom

    for b in range(draws):  # Evaluate the moments on the grid
        # print("DRAW"+str(b))
        mm = simulate_moments(
            pd.DataFrame(
                data=params_draws[b], index=list(msm_params.index), columns=["value"]
            ),
            n_draws=10_000,
            seed=0,
        )
        moms[b] = mm
        # Vs[b] = V
        W, internal_weights = get_weighting_matrix(  # weighting matrix
            moments_cov=moments_cov,
            method=weights,
            empirical_moments=mm,
            return_type="pytree_and_array",
        )

        # objs[b] = get_msm_optimization_functions(simulate_moments,empirical_moments,W)
        objs[b] = np.dot(mm.T, np.linalg.solve(W, mm))

    # Select draws on the level set
    ind = np.array(objs) - min(np.array(objs)) <= bandwidth
    # why it was np.array(objs) - min(np.array(objs)) in the code? according to
    # the paper it should be objs <= bandwidth
    ind = [i for i, x in enumerate(ind) if x]
    grid_sub = params_draws[ind]
    moms_sub = np.array([moms[i] for i in ind])
    # Vs_sub = [V for i in ind]

    return grid_sub, moms_sub


def calculate_quasi_jacobian(grid_sub, moms_sub, n_params):

    """Calculates the quasi-Jacobean matrix as the slope of a linear
    approximation of the moments on an estimate of the identified set.
    It is asymptotically singular when local and/or global identification
    fails, and equivalent to the usual Jacobean matrix which has full rank
    when the model is points and locally identified.

    Args:
        grid_sub (list): The selected draws.
        moms_sub (array): Simulated moments for the selected draws.
        n_params (int): Number of parameters in the model.

    Returns:
        The quasi-Jacobean matrix and the inverse square root variance matrix.

    """

    # 1. CALCULATE QUASI-JACOBIAN (THE SLOPE IN THE L-oo REGRESSION)

    # the kernel ???
    X = np.column_stack(
        [[1] * len(grid_sub), grid_sub]
    )  # regressors: intercept and theta_b

    beta = cp.Variable(
        shape=(X.shape[1], moms_sub.shape[1])
    )  # matrix of coefficients (A,B)
    objc = cp.Minimize(cp.norm(moms_sub - X @ beta, p="inf"))  # 1 - infinity loss
    prob = cp.Problem(objc)  # compile the problem
    prob.solve(solver="ECOS")
    # compute the solution
    solution = beta.value  # extract the solution

    Bn = solution[
        1:,
    ]  # qusi-Jacobian

    # 2. COMPUTE VARIANCE (REPARAMETRIZED)
    mu = cp.Variable(shape=(1, n_params))  # vector of means 1xnumber of parameters
    one = np.full((1, len(grid_sub)), 1)
    VV = cp.Variable(shape=(n_params, n_params))  # matrix of variances

    # grid_sub.T@VV
    # cp.norm( (grid_sub@VV - cp.kron(one,mu.T))**2,'inf')
    objc = cp.Minimize(
        -cp.log_det(VV)
        + 0.5 * cp.norm((grid_sub @ VV - cp.kron(one, mu.T).T) ** 2, "inf")
    )  # !!!ADDED TRANSPOSITION (doesnot matter which to transpose)
    prob = cp.Problem(objc)
    prob.solve(solver="SCS")
    # compute the solution #DEPEDS ON THE SOLVER
    phi = beta.value  # extract the solution
    # Note that phi = Sigma^(-1/2), the problem was reparametrized
    # phi

    return Bn, phi


# V - moments_cov
def category_selection(moments_cov, n_params, Bn, phi, cutoff):

    """Computes the singular values for normalized quasi-Jacobean.
    Selects the number of singular values larger than cutoff value.

    Args:
        moments_cov (array): An array containing the covariance matrix of
            the empirical moments. This is typically calculated with
            our ``get_moments_cov`` function.
        n_params (int): Number of parameters in the model.
        Bn (array): Quasi-Jacobean matrix which is calculated with
            ``calculate_quasi_jacobian``.
        phi (array): Inverse square root variance matrix which is calculated
            with``calculate_quasi_jacobian``.
        cutoff (int): The cutoff for the identification category
        selection. By default, calculated as sqrt(2*math.log(n)/n).

    Returns:
        The singular values for normalized quasi-Jacobean, the cutoff and
        the number of identified parameters

    """

    # 1. CALCULATE NORMALIZED QUASI-JACOBEAN

    # 1.1 Compute the normalization matrix for the left-hand-side
    # (in the paper - Compute V_bar the average variance matrix)
    # V = np.zeros(shape=(len(Vs_sub[0]),len(Vs_sub[0]))) # variance for moments,
    # dimensions as much as moments
    # for b in range(len(grid_sub)):
    #    V = V + Vs_sub[b] / len(grid_sub)

    # 1.2 Computed the normalization matrix for the right-hand-side in step 2.ii) - phi

    v = np.array(
        [1 if i == 0 else 0 for i in range(n_params)]
    )  # vector which spans theta1
    M = np.identity(n_params) - np.dot(
        v, v.T
    )  # Projection matrix onto the span on theta2

    # 1.3 Normalized quasi-Jacobean
    import scipy

    # all random transpositions!!!!!!!!!! from left to right
    # np.dot(np.dot(np.dot(np.dot(scipy.linalg.inv(scipy.linalg.sqrtm(V)),Bn.T),M).T,phi.T).T,M)
    V_1_2 = scipy.linalg.inv(scipy.linalg.sqrtm(moments_cov))
    V_Bn = np.dot(V_1_2, Bn.T)
    V_Bn_P = np.dot(V_Bn, M)
    V_Bn_P_phi = np.dot(V_Bn_P.T, phi.T)
    V_Bn_P_phi_P = np.dot(V_Bn_P_phi.T, M)
    Bnorm = V_Bn_P_phi_P

    # 2. COMPUTE SINGULAR VALUES AND CUTOFF
    # singular values in a decreasing order
    sing = scipy.linalg.svd(Bnorm)[1]
    n_sing = sum(sing > cutoff)  # number of identified parameters

    return sing, cutoff, n_sing
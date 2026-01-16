def polar_histogram():
    """Construct a polar histogram given samples from a Von Mises distribution.

    Args:

    Examples:
        See: https://stackoverflow.com/a/67286236/446907
        >>> from scipy.special import i0
        >>> import numpy as np
        >>> import matplotlib.pyplot as plt

        >>> # Von-Mises distribution parameters
        >>> mu = -0.343
        >>> kappa = 10.432

        >>> # Construct random Von Mises distribution based on Mu and Kappa values
        >>> r = np.random.vonmises(mu, kappa, 1000)

        >>> # Adjust Von-Mises curve from fitted data
        >>> x = np.linspace(-np.pi, np.pi, num=501)
        >>> y = np.exp(kappa*np.cos(x-mu))/(2*np.pi*i0(kappa))

        >>> # From the data above (mu, kappa, x and y)
        >>> nbins = 100
        >>> theta = np.linspace(-np.pi, np.pi, num=nbins, endpoint=False)
        >>> radii = np.exp(kappa * np.cos(theta - mu)) / (2 * np.pi * i0(kappa))

        >>> # Display width
        >>> width = (2 * np.pi) / nbins

        >>> # Construct ax with polar projection
        >>> ax = plt.subplot(111, polar=True)
        >>> # Disable radial tick labels
        >>> ax.set_yticklabels([])
        >>> # Disable angular tick labels
        >>> ax.set_xticklabels([])

        >>> # Set Orientation
        >>> ax.set_theta_zero_location('E')
        >>> ax.set_theta_direction(-1)
        >>> ax.set_xlim(-np.pi/1.000001, np.pi/1.000001)  # workaround
        >>> ax.set_xticks([-np.pi/1.000001 + i/8 * 2*np.pi/1.000001 for i in range(8)])

        >>> # Plot bars
        >>> bars = ax.bar(x=theta, height=radii, width=width, color=plt.cm.Greens(50))
        >>> # Plot Line:
        >>> line = ax.plot(x, y, linewidth=1, color=plt.cm.Greens(250), zorder=3)

        >>> # Grid settings
        >>> ax.set_rgrids(np.arange(.5, 1.6, 0.5), angle=0, weight='light', color='grey')
        >>> plt.savefig('polar_histogram.pdf', bbox_inches='tight')
    """
    pass
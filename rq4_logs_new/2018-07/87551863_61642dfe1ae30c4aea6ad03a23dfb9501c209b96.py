def read_data(fname):
    from numpy import loadtxt
    array1 = loadtxt(fname)
    return array1

def tanh(z):
    from numpy import exp
    return (exp(z) - exp(-z))/(exp(z) + exp(-z))

def find_velo(fl,array):
    nxx = len(array)
    for i in range(nxx):
        if array[i]>=fl:
            index = i-1
            break
    return index


def plot_data(save_fig,plot_density):
    import matplotlib.pylab as plt
    import matplotlib
    from numpy import linspace,pi,sqrt,exp
#     matplotlib.rcParams['text.usetex'] = True
#     matplotlib.rcParams['text.latex.unicode'] = True
#     matplotlib.rcParams['axes.labelsize'] = 17
#     matplotlib.rcParams['xtick.labelsize'] = 15
#     matplotlib.rcParams['ytick.labelsize'] = 15
#     matplotlib.rcParams['legend.fontsize'] = 15
    nx = 50
    ny = 50
    x = linspace(0.1, 3.0,nx)
    y = linspace(0.0,5.5,ny)
    plt.figure(3,figsize = (10,5))
    delta_s = 30.0
    flow_depth = linspace(0.5,10,nx)
    fac1 = 0.9
    fac2 = fac1
    fac3 = delta_s / pi
    v_dune = []
    v_antidune_l = []
    v_antidune_h =[]
#     for i in range(nx):
#         fd_index = find_velo(flow_depth[i],x*fac3)
#         # v_dune.append(sqrt(tanh(x[fd_index])/x[fd_index])*sqrt(9.81*x[fd_index]*fac3))
#         # v_antidune_l.append(sqrt(1/x[fd_index])*sqrt(9.81*x[fd_index]*fac3))
#         # v_antidune_h.append((sqrt(fac1*x[fd_index]* tanh(fac2*x[fd_index]))**(-1.0)-0.04)*sqrt(9.81*x[fd_index]*fac3))
#         v_dune.append(sqrt(tanh(x[fd_index])/x[fd_index]))
#         v_antidune_l.append(sqrt(1/x[fd_index]))
#         v_antidune_h.append((sqrt(fac1*x[fd_index]* tanh(fac2*x[fd_index]))**(-1.0)-0.04))
# 

    if plot_density == 1:
        dens1, dens1 = read_density(x,y)
        cmap = plt.get_cmap("Blues")
        plt.pcolor(x*delta_s/pi,y,dens1.T,cmap=cmap)
        plt.plot(x*delta_s/pi,(sqrt(fac1*x* tanh(fac2*x))**(-1.0)-0.04),'k-')
        plt.plot(x*delta_s/pi, sqrt(tanh(x)/x),'k-')
        plt.plot(x*delta_s/pi, sqrt(1/x),'k-')
        plt.ylim(0.,2.5)
        plt.xlim(x[0]*delta_s/pi,15.)
        # plt.pcolor(x,y,dens1.T,cmap=cmap)
        # plt.plot(x,(sqrt(fac1*x* tanh(fac2*x))**(-1.0)-0.04)*sqrt(9.81*x*fac3),'k-')
        # plt.plot(x, sqrt(tanh(x)/x)*sqrt(9.81*x*fac3),'k-')
        # plt.plot(x, sqrt(1/x)*sqrt(9.81*x*fac3),'k-')
#         plt.plot(flow_depth,v_dune,'b-',alpha=0.5)
#         plt.plot(flow_depth,v_antidune_l,'r-',alpha=0.5)
#         plt.plot(flow_depth,v_antidune_h,'r-',alpha = 0.5)
#         plt.plot(x,v_dune,'b-',alpha=0.5)
#         plt.plot(x,v_antidune_l,'r-',alpha=0.5)
#         plt.plot(x,v_antidune_h,'r-',alpha = 0.5)
    # plt.fill_between(flow_depth, v_antidune_l,v_antidune_h,color='r',alpha=0.5)
    # plt.fill_between(flow_depth, 0,v_dune,color='b',alpha=0.5)
    # plt.xlim(0.5,5.0)
    # plt.ylim(2,18)
    # plt.xlabel(r'$\textrm{Flow depth,}\; m$')
    # plt.ylabel(r'$\textrm{Flow Speed,}\; m/s$')
    # title_text = '$\delta_s = {test1:3.1f} $'.format(test1=delta_s)
    # plt.title(title_text,size =20)
    # plt.text(3.0,3.0,r'$\textrm{Dunes}$', size=20)
    # plt.text(0.6,8.0,r'$\textrm{Antidunes}$', size=20)
#     if save_fig == 1:
#         plt.savefig('flowspeed.png',dpi=501, bbox_inches="tight")
#     else:
        plt.show()



def read_density(x,y):
    from progress.bar import Bar
    from numpy import zeros,amax
    import numpy.ma as ma
    fname1 = 'snorm_dunes.dat'
    fname2 = 'santi_dunes.dat'
    snd = read_data(fname1)
    sad = read_data(fname2)
    fname3 = 'unorm_dunes.dat'
    fname4 = 'uanti_dunes.dat'
    und = read_data(fname3)
    uad = read_data(fname4)

    nx = len(x)
    ny = len(y)
    arr1 = snd
    arr2 = sad
    arr3 = und
    arr4 = uad
    len_arr1 = len(arr1[:,3])
    len_arr2 = len(arr2[:,3])
    len_arr3 = len(arr3[:,3])
    len_arr4 = len(arr4[:,3])

    bar = Bar('Processing Data', max=nx*ny, suffix='%(index)d/%(max)d - %(percent).1f%% - %(eta)ds')
    # x = linspace(0.1, 3.0,nx)
    # y = linspace(0.1,2.5,ny)
    # dx = x[1] - x[0]
    # dy = y[1] - y[0]
    density1 = (nx,ny)
    density1 = zeros(density1)
    density2 = (nx,ny)
    density2 = zeros(density2)
    density3 = (nx,ny)
    density3 = zeros(density3)
    density4 = (nx,ny)
    density4 = zeros(density4)
    for i in range(0,nx-1):
        #print i
        for j in range(0,ny-1):
            m1 = 0
            for k in range(len_arr1):
                if arr1[k,3] > x[i]  and arr1[k,3]<= x[i+1]:
                    if  arr1[k,1] >= y[j] and arr1[k,1]< y[j+1]:
                        m1 += 1
            density1[i,j] = m1
            m2 = 0
            for k in range(len_arr2):
                if arr2[k,3] > x[i]  and arr2[k,3]<= x[i+1]:
                    if  arr2[k,1] >= y[j] and arr2[k,1]< y[j+1]:
                        m2 += 1
            density2[i,j] = m2
            #m3 = 0
            for k in range(len_arr3):
                if arr3[k,3] > x[i]  and arr3[k,3]<= x[i+1]:
                    if  arr3[k,1] >= y[j] and arr3[k,1]< y[j+1]:
                        m1 += 1
            density1[i,j] = m1
            m4 = 0
            for k in range(len_arr4):
                if arr4[k,3] > x[i]  and arr4[k,3]<= x[i+1]:
                    if  arr4[k,1] >= y[j] and arr4[k,1]< y[j+1]:
                        m4 += 1
            density4[i,j] = m4
            bar.next()
    bar.finish()
    print(amax(density1))
    density1 = density1/amax(density1)
    print(amax(density1))
    density1[density1<0.1] = 'NaN'
    density1 = ma.masked_invalid(density1)
    density4 = density4/amax(density4)
    density4[density4<0.1] = 'NaN'
    density4 = ma.masked_invalid(density4)
    return density1, density4
import numpy as np
import TimberManager as tm

class FBCT:
    def __init__(self, timber_variable, beam=0, device='A', bct_for_norm=None):

        if type(timber_variable) is str:
            if not (beam == 1 or beam == 2):
                raise ValueError('You need to specify which beam! (1 or 2)')
            dict_timber = tm.parse_timber_file(timber_variable, verbose=True)
            timber_variable_FBCT = dict_timber[get_variable_dict(beam)['BUNCH_INTENSITY_' + device]]
        elif hasattr(timber_variable, '__getitem__'):
            timber_variable_FBCT = timber_variable[get_variable_dict(beam)['BUNCH_INTENSITY_' + device]]            

        self.t_stamps = timber_variable_FBCT.t_stamps
        self.bint = timber_variable_FBCT.values	
	     
        # self.bint = map(lambda x: np.array(map(float, x)), self.bint)
        # self.bint = np.array(self.bint)
        # self.t_stamps = np.array(self.t_stamps)
        # self.totint = np.array(map(sum, self.bint))

	self.bint = np.array(np.float_(self.bint))
	self.t_stamps = np.array(np.float_(self.t_stamps))
	self.totint = np.sum(self.bint, axis = 1)
	
	
	if bct_for_norm:
	    bct_int_interp = np.array(np.interp(self.t_stamps, bct_for_norm.t_stamps, bct_for_norm.values)) 	
   	    fbct_int = self.totint
            cor_fact=(fbct_int/bct_int_interp)

	    fbct_bint_ren = []
	    t_stamps_ren = []
	    for ii in range(self.bint.shape[0]):
		if cor_fact[ii] != 0:	    	
		    fbct_bint_ren.append(self.bint[ii,:]/cor_fact[ii])
		    t_stamps_ren.append(self.t_stamps[ii])

	    self.bint = np.array(fbct_bint_ren)
	    self.t_stamps = np.array(t_stamps_ren)
	    self.totint = np.sum(self.bint, axis = 1)
	    

    def uniform_time(self, t_inter=60.):
        t_stamps = self.t_stamps
        bint = self.bint
        nslots = bint.shape[1]

        t_stamps_unif = np.arange(np.min(t_stamps), np.max(t_stamps), t_inter)
        bint_unif = 0.*np.zeros((len(t_stamps_unif), nslots))
        for ii in xrange(nslots):
            bint_unif[:,ii] = np.interp(t_stamps_unif, t_stamps, bint[:,ii])

        return t_stamps_unif, bint_unif


    def nearest_older_sample(self, t_obs, flag_return_time=False):
        ind_min = np.argmin(np.abs(self.t_stamps - t_obs))
        if self.t_stamps[ind_min] > t_obs:
            ind_min -= 1
        if flag_return_time:	
            if ind_min == -1:
                return 0.*self.bint[ind_min], -1
            else:	
                return self.bint[ind_min], self.t_stamps[ind_min]
        else:
            if ind_min == -1:
                return 0.*self.bint[ind_min]
            else:	
                return self.bint[ind_min]
                

		
def get_variable_dict(beam):
    var_dict = {}
    var_dict['BUNCH_INTENSITY_A'] = 'LHC.BCTFR.A6R4.B%d:BUNCH_INTENSITY'%beam
    var_dict['BUNCH_INTENSITY_B'] = 'LHC.BCTFR.B6R4.B%d:BUNCH_INTENSITY'%beam

    return var_dict

def variable_list(beams = [1,2]):
    var_list = []
    for beam in beams:
        var_list += get_variable_dict(beam).values()

    return var_list
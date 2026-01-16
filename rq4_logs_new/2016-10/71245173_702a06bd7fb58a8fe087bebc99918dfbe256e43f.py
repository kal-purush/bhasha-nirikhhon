#!/usr/bin/python2.7
# -*-coding:Utf-8 -*

from math import *
import numpy as np
import datetime

from scipy.optimize import curve_fit
from scipy.optimize import leastsq
from scipy.optimize import minimize
from scipy.optimize import fmin_slsqp
import scipy.interpolate as inter

from datetime import datetime

import time
import sys
sys.path.insert(0, '/ams/aupetit/Documents/Pheno/Code_Python/Lib/')
from Physics import *
from ExtractData import *
from PlotLib import *

import matplotlib.pyplot as plt
from matplotlib.dates import YearLocator, YEARLY, MonthLocator, RRuleLocator, rrulewrapper, DateFormatter
from matplotlib import rc
rc('font', **{'family': 'serif', 'serif': ['Computer Modern']})
rc('text', usetex=True)

np.set_printoptions(precision=4, linewidth=200)

#---------------------------------------------------------------
def main(directory):

	# Load experiments
	list_exp = ['AMS01','AMS02', 'BESS97', 'BESS98', 'BESS99', 'BESS00', 'BESSPOLAR1', 'BESSPOLAR2', 'PAMELA0608', 'PAMELA2006', 'PAMELA2007', 'PAMELA2008', 'PAMELA2009']	
	Nexp = len(list_exp)

	#------------------------------------------
	# Comparison between J_IS(FF) and J_IS(1D)
	#------------------------------------------

	file_FF_IS 	= directory + "final_results/BestFitFF_all_IS_up.txt"
	file_1D_IS 	= directory + "final_results/BestFit1D_all_IS_up.txt"
	IS_FF 	= np.loadtxt(file_FF_IS)
	IS_1D 	= np.loadtxt(file_1D_IS)
	
	E_IS = IS_1D[:,0]
	flux_1D_IS = IS_1D[:,1]
	flux_FF_IS = IS_FF[:,1]

	ratio_JIS = np.zeros(len(flux_1D_IS))
	for i in range (0, len(flux_1D_IS)):
		ratio_JIS[i] = float(flux_1D_IS[i]) / float(flux_FF_IS[i])

	max_IS = max(ratio_JIS)
	ratio_JIS /= max_IS 		# Renormalisation a cause du pb sur J_IS(1D) a haute energie

	# Bandes d'incertitudes
	uncert_IS = np.loadtxt(directory + "Data/Uncertainties_Ghelfi/uncertainties_IS.dat")
	x1 = uncert_IS[:,0]
	ymed = uncert_IS[:,1] 
	yup1 = uncert_IS[:,2]
	ylow1 = uncert_IS[:,3]
	
	ymed1 = ymed /ymed
	yup1 /= ymed
	ylow1 /= ymed

	# Plot
	fig_IS = SetFig()
	plot_ratio = plt.plot(E_IS, ratio_JIS, 'r--', lw = 2.5, ms=3.)
	plt.plot(x1, yup1, c='black', lw = 2., label=r"68\% confidence-level from [1]")
	plt.plot(x1, ylow1, c='black', lw = 2.)
	SetLegend()
 	SetAxis(r"${\rm{}E}_{k/n} \rm{[GeV/n]}$", r"$\rm{J^{IS}_{1D} / J^{IS}_{FF}}$", 0.5, 1.5e3, "None", "None", "xlog")
 	#SaveFig("plots","test", "png")

 	#----------------------------------------------------
	# Reidual between J_TOA / Jdata for FF and 1D models
	#----------------------------------------------------

	file_FF_TOA = directory + "final_results/BestFitFF_all_TOA.txt"
	file_1D_TOA = directory + "final_results/BestFit1D_all_TOA.txt"
	TOA_FF 	= np.loadtxt(file_FF_TOA)
	TOA_1D 	= np.loadtxt(file_1D_TOA)
		
	E_TOA = TOA_1D[:,0]
	flux_1D_TOA , flux_FF_TOA =  ([] for _ in xrange(2))
	for i in range(0, Nexp):
		flux_1D_TOA.append(TOA_1D[:,i+1])
		flux_FF_TOA.append(TOA_FF[:,i+1])
	
	ratio_list_FF_TOA, ratio_list_1D_TOA = ([] for _ in xrange(2)) 
	Edata, ydata, sigma = ([] for _ in xrange(3))
	
	i = 0
	for name in list_exp :
		i += 1
		data = np.loadtxt(directory + 'Data/data_' + name + '.dat')
		Edata.append(data[:,0])
		ydata.append(data[:,3])
		sigma.append(data[:,9])
		
		flux_1D_TOA_interp = np.interp(Edata[i-1], E_TOA, flux_1D_TOA[i-1])
		flux_FF_TOA_interp = np.interp(Edata[i-1], E_TOA, flux_FF_TOA[i-1])
		
		ratio_1D_TOA = np.zeros(len(Edata[i-1]))
		ratio_FF_TOA = np.zeros(len(Edata[i-1]))
		for j in range(0,len(Edata[i-1])):
			ratio_1D_TOA[j] = flux_1D_TOA_interp[j] / ydata[i-1][j]
			ratio_FF_TOA[j] = flux_FF_TOA_interp[j] / ydata[i-1][j]
		ratio_list_1D_TOA.append(ratio_1D_TOA)
		ratio_list_FF_TOA.append(ratio_FF_TOA)


	uncert_pamela = np.loadtxt(directory + "final_results/uncertainties_PAMELA2006.dat")
	x_pam = uncert_pamela[:,0]
	yup_pam = uncert_pamela[:,1]
	ylow_pam = uncert_pamela[:,2]
	uncert_ams = np.loadtxt(directory + "final_results/uncertainties_AMS02.dat")
	x_ams = uncert_ams[:,0]
	yup_ams = uncert_ams[:,1]
	ylow_ams = uncert_ams[:,2]

	# Plot
	f, (ax1, ax2) = plt.subplots(2, sharex=True, sharey=True)
	f.set_facecolor('white')	
	for i in range(0, Nexp):
		if list_exp[i] == "AMS02" or list_exp[i] == "PAMELA2006" :
			if list_exp[i] == "AMS02" :
				ax1.plot(Edata[i], ratio_list_1D_TOA[i], 'bo', ms=5., label = "1D model")
				ax1.plot(Edata[i], ratio_list_FF_TOA[i], 'rD', ms=5., label= "Force field")
			if list_exp[i] == "PAMELA2006" :
				ax2.plot(Edata[i], ratio_list_1D_TOA[i], 'bo', ms=5., label = "1D model")
				ax2.plot(Edata[i], ratio_list_FF_TOA[i], 'rD', ms=5., label= "Force field")
			# Fine-tune figure; make subplots close to each other and hide x ticks for
			# all but bottom plot.
			if list_exp[i] == "PAMELA2006":
				ax2.plot(x_pam, yup_pam, c='black', lw = 2.5, label="Data uncertainties")
				ax2.plot(x_pam, ylow_pam, c='black', lw = 2.5)
			if list_exp[i] == "AMS02":
				ax1.plot(x_ams, yup_ams, c='black', lw = 2.5, label="Data uncertainties")
				ax1.plot(x_ams, ylow_ams, c='black', lw = 2.5)

	SetLegendSub(ax1, 2)
	SetAxisSub(ax1, ax2, r"$\rm{E_{k/n} [GeV/n]}$", r"$\rm{J^{TOA}_{best-fit} / J_{data}}$", 5e-2, 2e3, 0.85, 1.2, "sharex", "xlog")
	plt.locator_params(axis='y',nbins=4)
	plt.show()

#---------------------------------------------------------------	
def Comp_NM(directory):

	#PAMELA 

	phi_NM_data_PAMELA = directory + "Data/NM/NM_PAMELA.dat"	
	phi_NM_PAMELA = np.loadtxt(phi_NM_data_PAMELA, dtype= "str")
	dates_PAMELA = phi_NM_PAMELA[:,0]
	phi_PAMELA = phi_NM_PAMELA[:,2]
	
	dates_PAMELA = dates_PAMELA.astype(np.datetime64)
	dates_PAMELA = dates_PAMELA.astype(datetime.datetime)
	phi_PAMELA = phi_PAMELA.astype(np.float)
	
	phi_PAMELA_err_up = phi_PAMELA + 0.1 * phi_PAMELA
	phi_PAMELA_err_low = phi_PAMELA - 0.1 * phi_PAMELA
	
	name_exp_list_PAMELA = []
	for i in range(1,46):
		name_exp_list_PAMELA.append("PAMELA%i" % i)

	date_list_mean_PAMELA, date_list_delta_PAMELA = ExtractDate(name_exp_list_PAMELA)
	
	phi_mensuel_FF_PAMELA = np.loadtxt('../Force_Field/phi_PAMELA_mensuel.dat', dtype= "float") 
	#phi_mensuel_1D = np.loadtxt('../SolMod1D/phi_PAMELA_mensuel.dat', dtype= "float") 

	#--------
	# Plots
	#--------

	fig2, ax2 = plt.subplots(1,1,figsize=(12,8))
	fig2.set_facecolor('white')

	plt.plot(dates_PAMELA, phi_PAMELA, c='black', lw = 2.5, label='From NM Kiel data')
	plt.plot(dates_PAMELA, phi_PAMELA_err_up, c='grey', lw = 1.5)#, label=r'10\% uncertainties (Ghelfi et al (2016b))')
	plt.plot(dates_PAMELA, phi_PAMELA_err_low, c='grey', lw = 1.5)
	ax2.fill_between(dates_PAMELA, phi_PAMELA_err_up, phi_PAMELA_err_low, where=phi_PAMELA_err_up >= phi_PAMELA_err_low, facecolor='lightgrey')
    
	for i in range(0, len(name_exp_list_PAMELA)):
		if i==0 :
			ax2.errorbar(date_list_mean_PAMELA[i], phi_mensuel_FF_PAMELA[i], xerr = date_list_delta_PAMELA[i], yerr = 0.05*phi_mensuel_FF_PAMELA[i] , fmt='o', c='red', label = "From PAMELA data")  # fmt='o--',
		else : 
			ax2.errorbar(date_list_mean_PAMELA[i], phi_mensuel_FF_PAMELA[i], xerr = date_list_delta_PAMELA[i], yerr = 0.05*phi_mensuel_FF_PAMELA[i], fmt='o', c='red') 
		#ax.errorbar(date_list_mean[i], phi_mensuel_1D[i], xerr = date_list_delta[i], yerr = 0.02, fmt='o', c='blue')  # fmt='o--',
		
	#for i in range(0,Nexp):
	#	ax.annotate(list_exp[i], (date_list_mean[i],best_phi[i]+0.05))

	SetAxis(r"Time",r"$\phi \rm{[GV]}$", datetime.date(2006, 6, 1), datetime.date(2009, 2, 1), 0.4, 0.8, "lin")
	SetDateAxis(ax2, "monthly")
	SetLegend()
	#SaveFig(directory + "final_results/", "NM_PAMELA", "png")

	#----------------------------------------

	#  AMS 

	phi_NM_data_AMS = directory + "Data/NM/NM_AMS.dat"	
	phi_NM_AMS = np.loadtxt(phi_NM_data_AMS, dtype= "str")
	dates_AMS = phi_NM_AMS[:,0]
	phi_AMS = phi_NM_AMS[:,2]
	
	dates_AMS = dates_AMS.astype(np.datetime64)
	dates_AMS = dates_AMS.astype(datetime.datetime)
	phi_AMS = phi_AMS.astype(np.float)
	
	phi_AMS_err_up = phi_AMS + 0.1 * phi_AMS
	phi_AMS_err_low = phi_AMS - 0.1 * phi_AMS
	

	# AMS mensuel

	name_exp_list_AMS = []
	for i in range(1,29):
		name_exp_list_AMS.append("AMS%i" % i)

	date_list_mean_AMS, date_list_delta_AMS = ExtractDate(name_exp_list_AMS)

	phi_mensuel_FF_AMS = np.loadtxt('../Force_Field/phi_AMS_mensuel.dat', dtype= "float") 
	#phi_mensuel_1D = np.loadtxt('../SolMod1D/phi_AMS_mensuel.dat', dtype= "float") 

	#--------
	# Plots
	#--------

	fig3, ax = plt.subplots(1,1,figsize=(12,8))
	fig3.set_facecolor('white')
    
	for i in range(1, len(name_exp_list_AMS)):
		if i==1 :
			ax.errorbar(date_list_mean_AMS[i], phi_mensuel_FF_AMS[i], xerr = date_list_delta_AMS[i], yerr = 0.05*phi_mensuel_FF_AMS[i] , fmt='o', c='red', label = "From AMS data")  # fmt='o--',
		else : 
			ax.errorbar(date_list_mean_AMS[i], phi_mensuel_FF_AMS[i], xerr = date_list_delta_AMS[i], yerr = 0.05*phi_mensuel_FF_AMS[i], fmt='o', c='red') 
		#ax.errorbar(date_list_mean[i], phi_mensuel_1D[i], xerr = date_list_delta[i], yerr = 0.02, fmt='o', c='blue')  # fmt='o--',

	plt.plot(dates_AMS, phi_AMS, c='black', lw = 2.5, label='From NM Kiel data')
	plt.plot(dates_AMS, phi_AMS_err_up, c='grey', lw = 1.5)#, label=r'10\% uncertainties from Ghelfi et al (2016b)')
	plt.plot(dates_AMS, phi_AMS_err_low, c='grey', lw = 1.5)
	ax.fill_between(dates_AMS, phi_AMS_err_up, phi_AMS_err_low, where=phi_AMS_err_up >= phi_AMS_err_low, facecolor='lightgrey')
	
	#for i in range(0,Nexp):
	#	ax.annotate(list_exp[i], (date_list_mean[i],best_phi[i]+0.05)) 

	SetAxis(r"Time", r"$\phi \rm{[GV]}$", datetime.date(2011, 2, 1), datetime.date(2014, 1, 1), 0.3, 1.1, "lin")
	SetDateAxis(ax, "monthly")
	SetLegend()
	#SaveFig(directory + "final_results/", "NM_AMS", "png")
	plt.show()


#---------------------------------------------------------------	

directory	= "/ams/aupetit/Documents/Pheno/Code_Python/"
#main(directory)
Comp_NM(directory)
#phiNM_AMS(directory)

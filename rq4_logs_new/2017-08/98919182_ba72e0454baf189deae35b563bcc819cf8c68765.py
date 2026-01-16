from django.shortcuts import render, redirect, get_object_or_404, reverse
from django.views.generic.base import TemplateView
from  django.views.generic.edit import CreateView
from django.http import HttpResponse, HttpResponseRedirect
from django.views import generic
from  django.contrib import messages
from django.core.urlresolvers import reverse_lazy
from .forms import CreateUserForm
from django.contrib.auth.models import User
from django.contrib.auth import get_user_model
from .models import Surveyee, SurveyTimes,EB, EH, EH_Question, ES_Question, ES, Tip_Question, Tip, EB_Question
from .models import EXF_Question,EXF, R, R_Question, SEF, SEF_Question, GR, GR_Question, SPR, SPR_Question,Total_for_Admin
from .models import SSP_Question, SSP, F, F_Question, GD, GD_Question, HM_Question, HM, HEALTH_Question, HEALTH, DM, DM_Question
from django.utils import timezone
from .readFile import readFile
import pandas as pd
import logging
from django_pandas.io import read_frame
import matplotlib
matplotlib.use("Agg")
import mpld3
from mpld3 import plugins
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import FormatStrFormatter



def summary(request, agent_id):

    def describe(df):
        data ={}
        fact_list=['Peb_all', 'Ehs_all','Ess_all', 'PSS' ]
        for i  in fact_list:
            data[i]=helper(df,i)
        return data


    def helper(df, str):
        pd.set_option('precision', 6 )
        mean = df[str].mean()
        count = df[str].count()
        min = df[str].min()
        max = df[str].max()
        std = df[str].std()
        desc ={'mean':mean, 'count':count, 'min':min, 'max':max, 'std':std}
        return desc

    all = Total_for_Admin.objects.all()
    df = read_frame(all)
    total = describe(df)

    users = User.objects.filter()
    list1=[]
    for i in users:
        if i.get_username() != 'yoonhas':
            agent = read_frame(Total_for_Admin.objects.filter(Agent=i.pk))
            p=(i.get_username(),i.id, describe(agent))
            list1.append(p)


    return render(request, "PSS/summary.html", {'agent_id':User.objects.get(id=agent_id).get_username(), 'total':total, 'indi':list1})

def draw_graph(box,agent_id):
    if agent_id ==1:
        PSS =pd.DataFrame(columns=['Health', 'Community','Childcare', 'Jobskills', 'SoftSkill', 'Peb_all',
                                   'Empowerment', 'Selfmotivation', 'SkilResources', 'GaolOrientation','Ehs_all',
                                   'Ess1', 'Ess2', 'Ess3', 'Ess4', 'Ess_all', 'PSS'])

        for i in range(4):
            _, diction = box[i]
            PSS.loc[i] = [diction['Health']['mean'],diction['Community']['mean'],diction['Childcare']['mean']
                ,diction['Jobskills']['mean'],diction['SoftSkill']['mean'],diction['Peb_all']['mean'],
                          diction['Empowerment']['mean'], diction['Selfmotivation']['mean'],
                          diction['SkilResources']['mean']
                , diction['GaolOrientation']['mean'], diction['Ehs_all']['mean'],
                          diction['Ess1']['mean'], diction['Ess2']['mean'], diction['Ess3']['mean']
                , diction['Ess4']['mean'], diction['Ess_all']['mean'], diction['PSS']['mean'] ]
    else:
        PSS = pd.DataFrame(columns=['Peb_all','Ehs_all', 'Ess_all', 'PSS'])

        for i in range(4):
            _, diction = box[i]
            PSS.loc[i] = [ diction['Peb_all']['mean'],
                          diction['Ehs_all']['mean'],
                          diction['Ess_all']['mean'], diction['PSS']['mean']]


    fig, ax = plt.subplots()
    line_collection= []
    x =['1st', '2nd', '3rd', '4th']
    ax.grid(True, alpha=0.3)
    for key, val in PSS.iteritems():

        l = ax.plot((val.index +1), val.values, label=key)
        line_collection.append(l)


    handles, labels = ax.get_legend_handles_labels()  # return lines and labels
    plt.xticks(val.index+1, x)

    interactive_legend = plugins.InteractiveLegendPlugin(line_collection,labels)
    plugins.connect(fig, interactive_legend)
    fig.subplots_adjust(right=0.7)
    ax.set_xlabel('Times')
    ax.set_ylabel('Mean')
    ax.set_title('PSS', size=20)
    html_fig = mpld3.fig_to_html(fig)

    plt.close(fig)

    return html_fig

def score_detail(request, agent_id):
    pd.set_option('precision', 6)
    def describe(df, agent_id):
        pd.set_option('precision', 6)
        data ={}
        if agent_id==1:
            fact_list=['Health', 'Community','Childcare', 'Jobskills', 'SoftSkill', 'Peb_all',
                       'Empowerment', 'Selfmotivation', 'SkilResources', 'GaolOrientation','Ehs_all',
                       'Ess1','Ess2','Ess3','Ess4', 'Ess_all', 'PSS', 'Health_Z', 'Community_Z','Childcare_Z', 'Jobskills_Z', 'SoftSkill_Z', 'Peb_all_Z',
                       'Empowerment_Z', 'Selfmotivation_Z', 'SkilResources_Z', 'GaolOrientation_Z','Ehs_all_Z',
                       'Ess1_Z','Ess2_Z','Ess3_Z','Ess4_Z', 'Ess_all_Z', 'PSS_Z']
        else:
            fact_list = ['Peb_all',
                         'Ehs_all',
                         'Ess_all', 'PSS', 'Peb_all_Z',
                            'Ehs_all_Z',
                         'Ess_all_Z', 'PSS_Z']

        for i  in fact_list:
            data[i]=helper(df,i)

        print (data)
        return data

    def helper(df, str):
        pd.set_option('precision', 6)
        mean =df[str].mean().round(10)
        count = df[str].count()
        min = df[str].min()
        max = df[str].max()
        std = df[str].std().round(6)
        desc ={'mean':mean, 'count':count, 'min':min, 'max':max, 'std':std}
        return desc

    def z_score(df):
        df['Health_Z']= (df.Health - df.Health.mean()) / df.Health.std(ddof=0)
        df['Community_Z'] = (df.Community - df.Community.mean()) / df.Community.std(ddof=0)
        df['Childcare_Z'] = (df.Childcare - df.Childcare.mean()) / df.Childcare.std(ddof=0)
        df['Jobskills_Z'] = (df.Jobskills - df.Jobskills.mean()) / df.Jobskills.std(ddof=0)
        df['SoftSkill_Z'] = (df.SoftSkill - df.SoftSkill.mean()) / df.SoftSkill.std(ddof=0)
        df['Peb_all_Z'] = (df.Peb_all - df.Peb_all.mean()) / df.Peb_all.std(ddof=0)

        df['Empowerment_Z'] = (df.Empowerment - df.Empowerment.mean()) / df.Empowerment.std(ddof=0)
        df['Selfmotivation_Z'] = (df.Selfmotivation - df.Selfmotivation.mean()) / df.Selfmotivation.std(ddof=0)
        df['SkilResources_Z'] = (df.SkilResources - df.SkilResources.mean()) / df.SkilResources.std(ddof=0)
        df['GaolOrientation_Z'] = (df.GaolOrientation - df.GaolOrientation.mean()) / df.GaolOrientation.std(ddof=0)
        df['Ehs_all_Z'] = (df.Ehs_all - df.Ehs_all.mean()) / df.Ehs_all.std(ddof=0)

        df['Ess1_Z'] = (df.Ess1 - df.Ess1.mean()) / df.Ess1.std(ddof=0)
        df['Ess2_Z'] = (df.Ess2 - df.Ess2.mean()) / df.Ess2.std(ddof=0)
        df['Ess3_Z'] = (df.Ess3 - df.Ess3.mean()) / df.Ess3.std(ddof=0)
        df['Ess4_Z'] = (df.Ess4 - df.Ess4.mean()) / df.Ess4.std(ddof=0)
        df['Ess_all_Z'] = (df.Ess_all - df.Ess_all.mean()) / df.Ess_all.std(ddof=0)
        df['PSS_Z'] = (df.PSS - df.PSS.mean()) / df.PSS.std(ddof=0)

        return df


    agent = User.objects.get(id=agent_id)
    i = 1
    list1=[]
    if agent_id == '1':
        while i != 5:
            pd.set_option('precision', 6)
            time = Total_for_Admin.objects.filter(Time=i)
            df = z_score(read_frame(time))
            list1.append((i, describe(df,agent_id)))
            i += 1
    else:
        while i != 5:
            pd.set_option('precision', 6)
            time = Total_for_Admin.objects.filter(Time=i, Agent=agent)
            df=z_score(read_frame(time))
            list1.append((i, describe(df,agent_id)))
            i += 1

    html_fig = draw_graph(list1, agent_id)
    return render(request, "PSS/score_detail.html", { 'agent':agent.get_username(), 'detail_list':list1, 'html_fig':html_fig})

def compare_view(request):
    agents = User.objects.all()
    return render(request, 'PSS/compare.html', {'agents':agents})

def compare_detail(request):

    def describe(df):
        pd.set_option('precision', 6)
        data = {}
        fact_list = ['Health', 'Community', 'Childcare', 'Jobskills', 'SoftSkill', 'Peb_all',
                     'Empowerment', 'Selfmotivation', 'SkilResources', 'GaolOrientation', 'Ehs_all',
                     'Ess1', 'Ess2', 'Ess3', 'Ess4', 'Ess_all', 'PSS']
        for i in fact_list:
            data[i] = helper(df, i)
        return data

    def helper(df, str):
        pd.set_option('precision', 6)
        mean = df[str].mean().round(6)
        count = df[str].count()
        min = df[str].min()
        max = df[str].max()
        std = df[str].std().round(6)
        desc = {'mean': mean, 'count': count, 'min': min, 'max': max, 'std': std}
        return desc

    raw_data = read_frame(Total_for_Admin.objects.all())
    races = request.POST['race']

    age = request.POST['age']
    if '-' in age:
        ages = list(age.split('-'))
        ageMask = (raw_data['DM12']>=ages[0])&(raw_data['DM12']<=ages[1])
    else:
        ages=age
        ageMask =(raw_data['DM12']==ages[0])


    year = request.POST['year']
    if '-' in year:
        years = list(year.split('-'))
        yearMask = (raw_data['DM12'] >= ages[0]) & (raw_data['DM12'] <= ages[1])
    else:
        years=year

    status = request.POST['status']
    date = request.POST['date']
    if '-' in date:
        dates = list(date.split('-'))
    else:
        dates = date

    begin = pd.to_datetime(dates[0], format='%Y-%m-%d' )
    end = pd.to_datetime(dates[1], format='%Y-%m-%d' )

    housing = request.POST['Housing']
    compare = request.POST.getlist('compare')

    print(ages)
    print(years)
    print(status)
    print(dates)
    print(begin, end)
    print(housing)
    print(compare)

    agent_list= {}
    for i in compare:
        if i == 1:
            if races != 77:
                raceMask = (raw_data['DM14']==races)



    return render(request, 'PSS/compare.html')


def show_graph(request):
    return
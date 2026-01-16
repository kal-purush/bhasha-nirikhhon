#-*-encodeing: utf8
import pandas as pd
import numpy as np
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
import sqlite3
import MySQLdb


def get_loan_batch(dt, pay_day):
    if dt.day < pay_day:
        tmp = str(dt)[0:7]
        return tmp.replace('-', '')
    else:
        dt1 = dt + relativedelta(months=1)
        tmp = str(dt1)[0:7]
        return tmp.replace('-', '')


def get_loan_schedule(**params):
    bal = params['balance']
    ir = params['yield']
    pay_freq = int(params['repaymentFrequency']) # 1, 3, 6, 12
    terms = params['maturity']/pay_freq
    loan_date = parse(params['loanRepayd'])
    pay_method = params['repaymentMethod']
    next_repayd = parse(params['next_repayd'])
    pay_day = parse(params['firstPayDate']).day
    delta = relativedelta(next_repayd, loan_date)
    if pay_method == u'一次性还本付息':
        principal = bal
        interest = bal*ir/12.0*(delta.years*12+delta.months)
        batch_no = get_loan_batch(next_repayd, pay_day)
        return {'batch_no': [batch_no],
                'principal': [principal],
                'interest': [interest]}
    pay_date_list = [next_repayd+relativedelta(months=pay_freq*k) for k in range(terms)]
    loan_batch_no = [get_loan_batch(k, pay_day) for k in pay_date_list]

    if pay_method == u'等额本息':
        principal = [round(abs(np.ppmt(ir/12.0*pay_freq, k+1, terms, bal)),2) for k in range(terms)]
        interest = [round(abs(np.ipmt(ir/12.0*pay_freq, k+1, terms, bal)),2) for k in range(terms)]
        return {'batch_no': loan_batch_no,
                'principal': principal,
                'interest': interest}
    if pay_method == u'等额本金':
        ppmt = bal*1.0/terms
        principal = [round(ppmt, 2)]*terms
        interest = [round((bal - ppmt*(k-1))*ir/12.0*pay_freq, 2) for k in range(terms)]
        return {'batch_no': loan_batch_no,
        'principal': principal,
        'interest': interest}
    if pay_method == u'先息后本':
        principal = [0]*terms
        principal[-1] = bal
        interest = [round(bal*ir/12.0*pay_freq, 2)]*terms
        return {'batch_no': loan_batch_no,
        'principal': principal,
        'interest': interest}
    if pay_method == u'等本等息':
        total_terms = terms + (delta.years*12+delta.months)/pay_freq - 1
        principal = [round(bal*1.0/terms, 2)]*terms
        interest = [round(bal*1.0/terms*total_terms*ir/12*pay_freq, 2)]
        return {'batch_no': loan_batch_no,
                'principal': principal,
                'interest': interest}


def get_loan_info(project_id):
    db = MySQLdb.connect('192.168.*.*', 'user', 'pwd', 'db')

    sql_cmd = """
            select balance, yield, repaymentFrequency, maturity, loanRepayd,
            repaymentMethod, next_repayd, firstPayDate from db0 where project_id = '{}'
            """.format(project_id)

    df = pd.read_sql(sql_cmd, con=db)
    df1 = df.apply(lambda x: get_loan_schedule(**x), 1).\
        apply(lambda x: zip(x['batch_no'], x['principal'], x['interest'])).sum()
    df2 = pd.DataFrame(df1, columns=['batch_no', 'principal', 'interest'])
    df3 = df2.groupby('batch_no').sum()
    df3 = df3.reset_index()
    df3['total'] = df3['principal'] + df3['interest']
    df3.sort_values('batch_no')
    sql_cmd1 = """
            select creditscore, riskscore, from db0 where project_id = '{}'
            """.format(project_id)
    df4 = pd.read_sql(sql_cmd1)
    df4['credit_score_range'] = df4.creditscore.apply(lambda x: min((int(x)-300)/60, 9))
    df4['risk_score_range'] = df4.riskscore.apply(lambda x: min(int(x)/10, 9))
    tmp = df4.groupby('credit_score_range')['credit_score'].count()
    tmp = tmp.reset_index()
    tmp.columns = ['score_range', 'counts']
    tmp.sort_values('score_range')
    tmp1 = df4.groupby('risk_score_range')['risk_score'].count()
    tmp1 = tmp1.reset_index()
    tmp1.columns = ['score_range', 'counts']
    tmp1.sort_values('score_range')


    dic = {
                'detail': [{
                            'type': "creditScore",
                            'data': {
                                    "creditScore": list(tmp.score_range),
                                    "credit_x": ['300-359', '360-419', '420-479', '480-539', '540-599',
                                                 '600-659', '660-719', '720-779', '780-839', '840-900']
                                    }
                            },
                            {
                            'type': "riskScore",
                            'data': {
                                    'riskScore': list(tmp1.score_range),
                                    'risk-x': ['0-9', '10-19', '20-29', '30-39', '40-49',
                                               '50-59', '60-69', '70-79', '80-89', '90-100']
                                    }
                            },
                            {
                            "type": "cash",
                            'data': {
                                    "prinpial": list(df3.principal),
                                    "benfit": list(df3.interest),
                                    "sum": list(df3.total),
                                    "cash-x": list(df3.batch_no)
                                    }
                            }]

            }

    return dic


def complete(d):
    d['total_due'] = d.principal_due + d.interest_due
    d['total_pay'] = d.principal + d.intereset
    d['cul_payment'] = d.principal.cumsum()
    d['remaining_balance'] = d.loan_total - d.cul_payment
    d['cul_principal_payment_th'] = d.loan_total - d.princial_due.cumsum()
    d['overdue_principal'] = d.principal_due.cumsum()-d.cul_payment
    overdue_flag = []
    flag = 0
    for k in d.overdue_principal:
        if abs(k) <= 0.01:
            flag = 0
            overdue_flag.append(flag)
        elif k > 0:
            if flag <= 0:
                flag = 1
                overdue_flag.append(flag)
            else:
                flag = flag + 1
                overdue_flag.append(flag)
        else:
            flag = -1
            overdue_flag.append(flag)
    d['overdue_remaining_bal'] = d.apply(lambda x: x.remaining_balance*int(x.overdue_flag>0),1)
    d['deliquency_flag'] = overdue_flag
    d['cushion_offset'] = d.total_pay - d.total_due
    d['start_balance'] = d.remaining_balance + d.principal
    return d


def payment_analysis(project_id, current_batch):
    db = MySQLdb.connect('192.168.*.*', 'user','pwd','db')
    sql_cmd = """
            select loan_id, term, batch_no, principal, interest, principal_due, interest_due, loan_total
            from db1 where batch_no<=current_batch and project_id={} and buyback_flag = 0""".format(project_id)

    df = pd.read_sql(sql_cmd, con=db)
    df1 = df.groupby('loan_id').apply(lambda x: complete(x))
    batch_x = list(set(list(df.batch_no)))
    batch_x.sort()
    df2 = df1.groupby('batch_no')['remaining_balance', 'cushion_offset', 'interest'
                                 'overdue_remaining_bal', 'total_pay', 'start_balance'].sum()
    df2 = df2.reset_index()
    df2 = df2.sort_values('batch_no')
    total_loan = df.groupby('loan_id')['loan_total'].max().sum()
    df2['CDR'] = df2.overdue_remaining_bal/total_loan
    df2['apr'] = 12 * df2.interest/df2.start_balance
    df2 = df2.fillna(0)
    dic = {
            'detail':[{
                'type': 'deposit',
                'data':{
                    'value': list(df2.cushion_offset),
                    'Division': batch_x
                    }
                },
                {
                    'type': 'balance',
                    'data': {
                        'value': list(df2.remaining_balance),
                        'Division': batch_x
                    }
                },
                {
                    'type': 'APR',
                    'data': {
                        'value': list(df2.apr),
                        'Division': batch_x
                    }

                },
                {
                    'type': 'CDR',
                    'data':{
                        'value': list(df2.CDR),
                        'Division': batch_x
                    }
                }]
                }
    return dic



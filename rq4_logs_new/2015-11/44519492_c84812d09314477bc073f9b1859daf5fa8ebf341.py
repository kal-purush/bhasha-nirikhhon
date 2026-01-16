def click_percentages(df): # show-in-service & URL mail clicks
    mail_events = df[(df.event=='LOGIN') & (df.event_target=='REPORT')]
    clickcount = float(mail_events.shape[0])
    print 'Total amount of email clicks: '+repr(clickcount)
    #print 'Amount of individual users : '+repr(mail_events.target_email.unique().shape[0])
    print 'Percentage of SHOW-IN-SERVICE clicks: '+repr(round(mail_events[mail_events.more_info=='SHOW-IN-SERVICE'].shape[0]/clickcount,3))
    print 'Percentage of URL clicks: '+repr(round(sum(mail_events.more_info.str.startswith('http://'))/clickcount,3))
    print
def calculate_direct_and_report_logins(df):
    """ Analyze login
      - Calculate direct and report logins (absolute numbers and ratios)

    """

    num_logins = len(df[(df.event == "LOGIN")])
    num_report_logins = len(df[(df.event == "LOGIN") & (df.event_target == "REPORT")])
    num_direct_logins = len(df[(df.event == "LOGIN") & (df.event_target != "REPORT")])

    ratio_report_logins = float(num_report_logins) / float(num_logins)
    ratio_direct_logins = float(num_direct_logins) / float(num_logins)

    print "Direct logins: {} ({} %)".format(num_direct_logins, round(ratio_direct_logins * 100, 4))
    print "Report logins: {} ({} %)".format(num_report_logins, round(ratio_report_logins * 100, 4))
    print "Total logins: {}".format(num_logins)
    print
    
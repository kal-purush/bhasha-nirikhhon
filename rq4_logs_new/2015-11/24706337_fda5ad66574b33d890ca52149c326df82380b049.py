from log import Log
from database import Database


if __name__ == "__main__":
    # Initialize
    logfile = Log('../logs/test.log')
    output = Log('../output/test.out')
    db = Database()
    db.connect()
    logfile.message('Initialization complete')

    # Get list of teams in MLS
    sql = ('SELECT teamname '
           'FROM tbl_teams '
           'WHERE League = %s '
           'ORDER BY teamname ASC')
    params = ('MLS', )
    rs = db.query(sql, params)

    for (team) in rs:
        output.message(team[0])

    logfile.message('Data query complete')

    logfile.message('Shutting down...')
    db.disconnect()
    output.end()
    logfile.end()

print('Finished!')
"""
Copyright ©2023. The Regents of the University of California (Regents). All Rights Reserved.

Permission to use, copy, modify, and distribute this software and its documentation
for educational, research, and not-for-profit purposes, without fee and without a
signed licensing agreement, is hereby granted, provided that the above copyright
notice, this paragraph and the following two paragraphs appear in all copies,
modifications, and distributions.

Contact The Office of Technology Licensing, UC Berkeley, 2150 Shattuck Avenue,
Suite 510, Berkeley, CA 94720-1620, (510) 643-7201, otl@berkeley.edu,
http://ipira.berkeley.edu/industry-info for commercial licensing opportunities.

IN NO EVENT SHALL REGENTS BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL,
INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF
THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF REGENTS HAS BEEN ADVISED
OF THE POSSIBILITY OF SUCH DAMAGE.

REGENTS SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE
SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY, PROVIDED HEREUNDER IS PROVIDED
"AS IS". REGENTS HAS NO OBLIGATION TO PROVIDE MAINTENANCE, SUPPORT, UPDATES,
ENHANCEMENTS, OR MODIFICATIONS.
"""

from contextlib import contextmanager
from datetime import datetime

from flask import current_app as app
import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool


connection_pool = None


def safe_execute_rds(string, **kwargs):
    global connection_pool
    if connection_pool is None:
        connection_pool = ThreadedConnectionPool(1, app.config['DATA_LOCH_MAX_CONNECTIONS'], app.config['DATA_LOCH_RDS_URI'])
    with cursor_from_pool() as cursor:
        return _safe_execute(string, cursor, **kwargs)


@contextmanager
def cursor_from_pool():
    try:
        connection = connection_pool.getconn()
        yield connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    finally:
        connection_pool.putconn(connection)


def _safe_execute(sql, cursor, **kwargs):
    try:
        ts = datetime.now().timestamp()
        cursor.execute(sql, kwargs)
        query_time = datetime.now().timestamp() - ts
    except psycopg2.Error as e:
        app.logger.error(f'SQL {sql} threw {e}')
        return None
    rows = [dict(r) for r in cursor.fetchall()]
    app.logger.debug(f'Query returned {len(rows)} rows in {query_time} seconds:\n{sql}\n{kwargs}')
    return rows


def get_current_term_index():
    rows = safe_execute_rds('SELECT * FROM terms.current_term_index')
    return None if not rows or (len(rows) == 0) else rows[0]


def get_undergraduate_term(term_id):
    sql = f"""SELECT * FROM terms.term_definitions
              WHERE term_id = '{term_id}'
           """
    return safe_execute_rds(sql)
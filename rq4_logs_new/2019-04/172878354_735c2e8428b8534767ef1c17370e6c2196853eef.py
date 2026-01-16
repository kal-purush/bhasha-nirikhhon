import os


def limit_offset_helper(limit, offset, order_by=None, extra_params=None):
    params = extra_params or {}
    clause = ""

    if order_by is not None:
        # NOTE; Order by should NEVER be user specified, as this would be
        # a SQL injection vulnerability.
        clause += " ORDER BY {0}".format(order_by)
    if limit is not None:
        clause += " LIMIT :limit"
        params['limit'] = limit
    if limit is not None and offset is not None:
        clause += " OFFSET :offset"
        params['offset'] = offset
    return clause, params


SQL_FOLDER = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), '..', 'sql'
)


def place_holder_generator(params):
    return " (" + ", ".join(("?" for _ in params)) + ") "
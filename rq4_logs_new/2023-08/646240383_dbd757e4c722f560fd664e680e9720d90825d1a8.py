from datetime import timedelta, datetime


def update_records_with_prev_predictions(records: list,
                                         last_weekday_preds: dict) -> None:
    
    """
    Update the records with the previous weekday's predictions. If the previous weekday's
    predictions are not available, set the previous weekday's prediction to -1.

    :param records: The records to update.
    :type records: list
    :param last_weekday_preds: The previous weekday's predictions.
    :type last_weekday_preds: dict

    :return: None
    """
    
    for record in records:
        for stock_prediction_today in record['predictions']:
            try:
                stock_prediction_today['prev_weekday_prediction'] = (last_weekday_preds[record['model']]
                                                                     [stock_prediction_today['ticker']])
            except KeyError:
                stock_prediction_today['prev_weekday_prediction'] = -1


def build_last_weekday_preds_dict(last_date: str, collection) -> dict:
    """
    Build a dictionary of the previous weekday's predictions.

    :param last_date: The last date in the database.
    :type last_date: str
    :param collection: The collection to query.
    :type collection: pymongo.collection.Collection

    :return: The previous weekday's predictions.
    :rtype: dict
    """

    prev_weekday_date = get_prev_weekday(last_date)
    prev_weekday_records = list(
        collection.find(
            {
                'workflow_date': prev_weekday_date
            }

        )
    )

    last_weekday_preds = {}

    for prev_weekday_record in prev_weekday_records:
        last_weekday_preds[prev_weekday_record['model']] = {}
        for prediction_entry in prev_weekday_record['predictions']:
            last_weekday_preds[prev_weekday_record['model']][prediction_entry['ticker']] = prediction_entry['prediction']

    return last_weekday_preds


def get_prev_weekday(last_date) -> str:
    """
    Get the previous weekday before the last date in the collection.

    :param last_date: The last date in the database.
    :type last_date: str
    """

    last_date_dt = datetime.strptime(last_date, '%m-%d-%Y')
    last_date_dt -= timedelta(days=1)
    while last_date_dt.weekday() > 4: # Mon-Fri are 0-4
        last_date_dt -= timedelta(days=1)

    return last_date_dt.strftime('%m-%d-%Y')
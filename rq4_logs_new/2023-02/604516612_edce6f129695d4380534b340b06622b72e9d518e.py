import ydb.iam

driver_config = {
    "endpoint": "grpcs://... your endpoint",
    "database": "/ru... your database",
    "credentials": ydb.iam.ServiceAccountCredentials.from_file(
        "./authorized_key.json"
    ),
    "root_certificates": ydb.load_ydb_root_certificate()
}

def create_table_example(session):
    session.create_table(driver_config['database'] + '/test',
                         ydb.TableDescription()
                         .with_column(ydb.Column('series_id', ydb.PrimitiveType.Uint64))
                         .with_column(ydb.Column('title', ydb.OptionalType(ydb.PrimitiveType.Utf8)))
                         .with_column(ydb.Column('release_date', ydb.OptionalType(ydb.PrimitiveType.Uint64)))
                         .with_primary_key('series_id'))

def upsert_example(session):
    session.transaction().execute(
        """
        UPSERT INTO test (series_id, title, release_date) VALUES
            (1, "Test title", 28);
        """.format(ydb.iam.ServiceAccountCredentials.from_file("./authorized_key.json")),
        commit_tx=True,
    )

def select_example(session):
    result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
        "SELECT * FROM test;".format(ydb.iam.ServiceAccountCredentials.from_file("./authorized_key.json")),
        commit_tx=True,
    )
    for row in result_sets[0].rows:
        print("series_id: ", row.series_id, ", title: ", row.title, ", release_date: ", row.release_date)


with ydb.Driver(**driver_config) as driver:
    try:
        driver.wait(fail_fast=True, timeout=1)
        session = driver.table_client.session().create()
        create_table_example(session)
        upsert_example(session)
        select_example(session)
        exit(1)
    except TimeoutError:
        print("Connect failed to YDB")
        print("Last reported errors by discovery:")
        print(driver.discovery_debug_details())
        exit(1)
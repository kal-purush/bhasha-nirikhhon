import pandas as pd
pd.options.mode.chained_assignment = None

df_source = pd.read_csv('data/csv_files/tofoto-kunder-behandlet.csv')

customers_cols = [
    'customers_id',
    'customers_gender',
    'customers_firstname',
    'customers_lastname',
    'customers_dob',
    'customers_email_address',
    'customers_default_address_id',
    'customers_seller_email',
    'customers_group_id',
    'customers_default_billing_address_id',
    'customers_telephone',
    'customers_fax',
    'customers_password',
    'customers_newsletter',
    'customers_notes',
    'customers_billing_email',
    'customers_bounce',
    'sales_rep'
]

address_book_cols = [
    'address_book_id',
    'customers_id',
    'entry_gender',
    'entry_company',
    'entry_company_co',
    'entry_firstname',
    'entry_lastname',
    'entry_street_address',
    'entry_suburb',
    'entry_postcode',
    'entry_city',
    'entry_state',
    'entry_country_id',
    'entry_zone_id',
    'entry_company_number'
]

customers_rows = []
address_book_rows = []

empty = ''

address_book_count = 5000


def is_empty(cell, alt, alt_true='0'):
    if pd.isna(cell):
        return alt
    else:
        if alt_true == '0':
            return cell
        else:
            return alt_true


for i in range(len(df_source)):
    customers_dict = {}
    address_book_default = {}
    address_book_billing = {}

    butikkadresse = False

    customers_dict['customers_id'] = df_source['id'][i]
    customers_dict['customers_gender'] = empty
    customers_dict['customers_firstname'] = is_empty(df_source['fornavn'][i], df_source['firmanavn'][i])
    customers_dict['customers_lastname'] = is_empty(df_source['etternavn'][i], empty)
    customers_dict['customers_dob'] = empty
    customers_dict['customers_email_address'] = is_empty(df_source['epost'][i], empty)

    if pd.notna(df_source['butikkadresse'][i]):
        butikkadresse = True
        customers_dict['customers_default_address_id'] = str(address_book_count)
    else:
        customers_dict['customers_default_address_id'] = is_empty(df_source['adresse'][i], empty, alt_true=str(address_book_count))

    customers_dict['customers_seller_email'] = empty
    customers_dict['customers_group_id'] = empty

    if butikkadresse:
        customers_dict['customers_default_billing_address_id'] = str(address_book_count + 1)
    else:
        customers_dict['customers_default_billing_address_id'] = is_empty(df_source['adresse'][i], empty, alt_true=str(address_book_count + 1))

    if pd.isna(df_source['mobil'][i]):
        customers_dict['customers_telephone'] = is_empty(df_source['tlf'][i], empty)
    else:
        customers_dict['customers_telephone'] = df_source['mobil'][i]

    customers_dict['customers_fax'] = empty
    customers_dict['customers_password'] = empty
    customers_dict['customers_newsletter'] = '0'
    customers_dict['customers_notes'] = empty
    customers_dict['customers_billing_email'] = is_empty(df_source['Faktura-epost'][i], empty)
    customers_dict['customers_bounce'] = empty
    customers_dict['sales_rep'] = empty

    # END OF CUSTOMERS, MOVING ONTO ADDRESS_BOOK

    # First default address:
    if customers_dict['customers_default_address_id'] != empty:
        address_book_default['address_book_id'] = str(address_book_count)
        address_book_default['customers_id'] = df_source['id'][i]
        address_book_default['entry_gender'] = empty
        address_book_default['entry_company'] = df_source['firmanavn'][i]
        address_book_default['entry_company_co'] = empty
        address_book_default['entry_firstname'] = is_empty(df_source['fornavn'][i], df_source['firmanavn'][i])
        address_book_default['entry_lastname'] = is_empty(df_source['etternavn'][i], empty)

        if butikkadresse:
            address_book_default['entry_street_address'] = df_source['butikkadresse'][i]
        else:
            address_book_default['entry_street_address'] = df_source['adresse'][i]

        address_book_default['entry_suburb'] = empty

        if butikkadresse:
            address_book_default['entry_postcode'] = is_empty(df_source['butikk_postnummer'][i], empty)
            address_book_default['entry_city'] = is_empty(df_source['butikk_sted'][i], empty)
        else:
            address_book_default['entry_postcode'] = is_empty(df_source['postnummer'][i], empty)
            address_book_default['entry_city'] = is_empty(df_source['sted'][i], empty)

        address_book_default['entry_state'] = empty

        if butikkadresse:
            if df_source['butikk_land'][i] == 'Norge':
                address_book_default['entry_country_id'] = '153'
                address_book_default['entry_zone_id'] = '1'
            else:
                address_book_default['entry_country_id'] = empty
                address_book_default['entry_zone_id'] = empty
        else:
            if df_source['land'][i] == 'Norge':
                address_book_default['entry_country_id'] = '153'
                address_book_default['entry_zone_id'] = '1'
            else:
                address_book_default['entry_country_id'] = empty
                address_book_default['entry_zone_id'] = empty

        address_book_default['entry_company_number'] = is_empty(df_source['org_nr'][i], empty)

    # Billing address:
    if customers_dict['customers_default_billing_address_id'] != empty:
        address_book_billing['address_book_id'] = str(address_book_count + 1)
        address_book_billing['customers_id'] = df_source['id'][i]
        address_book_billing['entry_gender'] = empty
        address_book_billing['entry_company'] = df_source['firmanavn'][i]
        address_book_billing['entry_company_co'] = empty
        address_book_billing['entry_firstname'] = is_empty(df_source['fornavn'][i], df_source['firmanavn'][i])
        address_book_billing['entry_lastname'] = is_empty(df_source['etternavn'][i], empty)
        address_book_billing['entry_street_address'] = df_source['adresse'][i]
        address_book_billing['entry_suburb'] = empty
        address_book_billing['entry_postcode'] = is_empty(df_source['postnummer'][i], empty)
        address_book_billing['entry_city'] = is_empty(df_source['sted'][i], empty)
        address_book_billing['entry_state'] = empty

        if df_source['land'][i] == 'Norge':
            address_book_billing['entry_country_id'] = '153'
            address_book_billing['entry_zone_id'] = '1'
        else:
            address_book_billing['entry_country_id'] = empty
            address_book_billing['entry_zone_id'] = empty

        address_book_billing['entry_company_number'] = is_empty(df_source['org_nr'][i], empty)

    customers_rows.append(customers_dict)
    if address_book_default:
        address_book_rows.append(address_book_default)
    if address_book_billing:
        address_book_rows.append(address_book_billing)

    address_book_count += 2

dfc = pd.DataFrame(customers_rows, columns=customers_cols)
dfa = pd.DataFrame(address_book_rows, columns=address_book_cols)

dfc.to_csv('data/output/tofotoas_customers.csv')
dfa.to_csv('data/output/tofotoas_address_book.csv')
print('Done.')
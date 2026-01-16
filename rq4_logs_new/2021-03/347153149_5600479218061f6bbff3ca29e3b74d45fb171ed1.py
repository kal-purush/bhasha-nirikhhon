from com.QnA_brute_force import QnABruteForce
from azure_data_scraper.az_product_info import AzProductInfo


def main():
    az = AzProductInfo(None, None, 'tests/tests.json')
    qna = QnABruteForce(az)

    print_product (qna, 'Full Service')
    print_product (qna, 'Empty Service')
    print_product (qna, 'Not GA Service')


def print_product (qna, product):
    
    # ------------------------------------------------------------------------------------------------------------------

    #print(product, ' - answer_is_ga\n', qna.answer_is_ga(product))
    #print('\n---------------------------------------------------------\n')
    #print(product, ' - answer_is_ga_in_cloud\n', qna.answer_is_ga_in_cloud(product, 'azure-public'))
    #print('\n---------------------------------------------------------\n')
    #print(product, ' - answer_is_ga_in_cloud\n', qna.answer_is_ga_in_cloud(product, 'azure-government'))
    #print('\n---------------------------------------------------------\n')

    # ------------------------------------------------------------------------------------------------------------------

    print(product, ' - answer_where_ga\n', qna.answer_where_ga(product))
    print('\n---------------------------------------------------------\n')
    print(product, ' - answer_where_ga_in\n', qna.answer_where_ga_in(product, 'azure-public'))
    print('\n---------------------------------------------------------\n')
    print(product, ' - answer_where_ga_in\n', qna.answer_where_ga_in(product, 'azure-government'))
    print('\n---------------------------------------------------------\n')

    # ------------------------------------------------------------------------------------------------------------------

    print(product, ' - answer_where_preview\n', qna.answer_where_preview(product))
    print('\n---------------------------------------------------------\n')
    print(product, ' - answer_where_preview_in\n', qna.answer_where_preview_in(product, 'azure-public'))
    print('\n---------------------------------------------------------\n')
    print(product, ' - answer_where_preview_in\n', qna.answer_where_preview_in(product, 'azure-government'))
    print('\n---------------------------------------------------------\n')

    # ------------------------------------------------------------------------------------------------------------------

    print(product, ' - answer_where_expected_ga\n', qna.answer_where_expected_ga(product))
    print('\n---------------------------------------------------------\n')
    print(product, ' - answer_where_expected_ga_in\n', qna.answer_where_expected_ga_in(product, 'azure-public'))
    print('\n---------------------------------------------------------\n')
    print(product, ' - answer_where_expected_ga_in\n', qna.answer_where_expected_ga_in(product, 'azure-government'))
    print('\n---------------------------------------------------------\n')

    # ------------------------------------------------------------------------------------------------------------------

    print(product, ' - answer_is_at_scope - il2\n', qna.answer_is_at_scope(product, 'il2'))
    print(product, ' - answer_is_at_scope - il4\n', qna.answer_is_at_scope(product, 'il4'))
    print(product, ' - answer_is_at_scope - il5\n', qna.answer_is_at_scope(product, 'il5'))
    print(product, ' - answer_is_at_scope - il6\n', qna.answer_is_at_scope(product, 'il6'))
    print(product, ' - answer_is_at_scope - FedRAMP Moderate\n', qna.answer_is_at_scope(product, 'FedRAMP Moderate'))
    print(product, ' - answer_is_at_scope - FedRAMP High\n', qna.answer_is_at_scope(product, 'FedRAMP High'))
    print('\n---------------------------------------------------------\n')

    # ------------------------------------------------------------------------------------------------------------------

    print(product, ' - answer_is_at_scope_in_cloud azure-public - il2\n', qna.answer_is_at_scope_in_cloud(product, 'il2', 'azure-public'))
    print(product, ' - answer_is_at_scope_in_cloud azure-public - il4\n', qna.answer_is_at_scope_in_cloud(product, 'il4', 'azure-public'))
    print(product, ' - answer_is_at_scope_in_cloud azure-public - il5\n', qna.answer_is_at_scope_in_cloud(product, 'il5', 'azure-public'))
    print(product, ' - answer_is_at_scope_in_cloud azure-public - il6\n', qna.answer_is_at_scope_in_cloud(product, 'il6', 'azure-public'))
    print(product, ' - answer_is_at_scope_in_cloud azure-public - FedRAMP Moderate\n', qna.answer_is_at_scope_in_cloud(product, 'FedRAMP Moderate', 'azure-public'))
    print(product, ' - answer_is_at_scope_in_cloud azure-public - FedRAMP High\n', qna.answer_is_at_scope_in_cloud(product, 'FedRAMP High', 'azure-public'))
    print('\n---------------------------------------------------------\n')

    print(product, ' - answer_is_at_scope_in_cloud azure-government - il2\n', qna.answer_is_at_scope_in_cloud(product, 'il2', 'azure-government'))
    print(product, ' - answer_is_at_scope_in_cloud azure-government - il4\n', qna.answer_is_at_scope_in_cloud(product, 'il4', 'azure-government'))
    print(product, ' - answer_is_at_scope_in_cloud azure-government - il5\n', qna.answer_is_at_scope_in_cloud(product, 'il5', 'azure-government'))
    print(product, ' - answer_is_at_scope_in_cloud azure-government - il6\n', qna.answer_is_at_scope_in_cloud(product, 'il6', 'azure-government'))
    print(product, ' - answer_is_at_scope_in_cloud azure-government - FedRAMP Moderate\n', qna.answer_is_at_scope_in_cloud(product, 'FedRAMP Moderate', 'azure-government'))
    print(product, ' - answer_is_at_scope_in_cloud azure-government - FedRAMP High\n', qna.answer_is_at_scope_in_cloud(product, 'FedRAMP High', 'azure-government'))
    print('\n---------------------------------------------------------\n')

    # answer_which_scopes(self, id)
    # answer_which_scopes_in_cloud(self, id, cloud)


main()
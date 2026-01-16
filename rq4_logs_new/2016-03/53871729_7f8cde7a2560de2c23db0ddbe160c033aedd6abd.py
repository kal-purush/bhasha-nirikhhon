from collections import OrderedDict
import suds.client

from thepay.utils import SignatureMixin


class DataApi(SignatureMixin):
    def __init__(self, config):
        """

        :param config: Config
        """
        self.config = config
        self.client = None

        self.connect()

    def connect(self):
        self.client = suds.client.Client(self.config.data_web_services_wsdl)

    def get_payment_methods(self):
        params = self._sign_params(OrderedDict((
            ('merchantId', self.config.merchant_id),
            ('accountId', self.config.account_id),
        )), self.config.data_api_password)
        return self.client.service.getPaymentMethods(**params).methods[0]

    def get_payment_state(self, payment_id):
        params = self._sign_params(OrderedDict((
            ('merchantId', self.config.merchant_id),
            ('paymentId', payment_id),
        )), self.config.data_api_password)
        return int(self.client.service.getPaymentState(**params).state)

    def get_payment(self, payment_id):
        params = self._sign_params(OrderedDict((
            ('merchantId', self.config.merchant_id),
            ('paymentId', payment_id),
        )), self.config.data_api_password)
        return self.client.service.getPayment(**params).payment

    def get_payment_instructions(self, payment_id):
        params = self._sign_params(OrderedDict((
            ('merchantId', self.config.merchant_id),
            ('paymentId', payment_id),
        )), self.config.data_api_password)
        return self.client.service.getPaymentInstructions(**params).paymentInfo

    def _format_datetime(self, value):
        return value.replace(microsecond=0).isoformat()

    def get_payments(self, value_from=None, value_to=None, created_on_from=None, created_on_to=None,
                     finished_on_from=None, finished_on_to=None, page=None):

        search_params = OrderedDict()

        if value_from is not None:
            search_params['valueFrom'] = value_from

        if value_to is not None:
            search_params['valueTo'] = value_to

        if created_on_from is not None:
            search_params['createdOnFrom'] = self._format_datetime(created_on_from)

        if created_on_to is not None:
            search_params['createdOnTo'] = self._format_datetime(created_on_to)

        if finished_on_from is not None:
            search_params['finishedOnFrom'] = self._format_datetime(finished_on_from)

        if finished_on_to is not None:
            search_params['finishedOnTo'] = self._format_datetime(finished_on_to)

        params = OrderedDict((
            ('merchantId', self.config.merchant_id),
        ))

        if search_params:
            params['searchParams'] = search_params

        if page:
            params['pagination'] = {'page': page}
        params['ordering'] = {'orderHow': 'ASC'}

        return self.client.service.getPayments(**self._sign_params(params, self.config.data_api_password))
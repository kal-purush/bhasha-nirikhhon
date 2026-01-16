# -*- coding: utf-8 -*-
"""module qatestlink.core.models.tl_reports"""


from .tl_models import ModelBase


class ReportBase(ModelBase):
    """TODO: doc class"""

    totals = None

    def __init__(self, properties, properties_int=None, properties_bool=None,
                 load=True):
        """TODO: doc method"""
        super(ReportBase, self).__init__(
            properties,
            properties_int=properties_int,
            properties_bool=properties_bool,
            load=load)
        self.totals = {}

    def __repr__(self):
        """Show basic properties for this object

        Returns:
            str -- format text with values for
                'ReportBase: totals:dict={}'
        """
        return "ReportBase: totals:dict={}".format(self.totals)


class RTPlanTotals(ReportBase):
    """TODO: doc class"""

    by_tester = None

    def __init__(self, properties, properties_int=None, properties_bool=None,
                 load=False):
        """TODO: doc method"""
        super(RTPlanTotals, self).__init__(
            properties,
            properties_int=properties_int,
            properties_bool=properties_bool,
            load=load)
        self.by_tester = list()
        # just load from this class instead of parent class
        self._load()

    def _load(self):
        super(RTPlanTotals, self)._load()
        for res_property in self._properties:
            name = self.convert_name(res_property['name'])
            value = res_property['value']
            if name == 'with_tester':
                with_tester_members = value['struct']['member']
                by_testers = list()
                for member in with_tester_members:
                    user_id = member['name']
                    report_totals_members = member.get(
                        'value')['struct']['member']
                    by_tester = {
                        "user_id": int(user_id),
                        "report_types": []
                    }
                    for member_total in report_totals_members:
                        report_type = member_total['name']
                        report_data_members = member_total.get(
                            'value')['struct']['member']
                        platform_id = None
                        report_type = None
                        qty = None
                        for report_data_member in report_data_members:
                            prop_name = report_data_member['name']
                            prop_value = report_data_member['value']
                            if prop_name == 'status':
                                report_type = prop_value['string']
                            if prop_name == 'platform_id':
                                try:
                                    platform_id = prop_value['string']
                                except KeyError:
                                    platform_id = prop_value['int']
                            if prop_name == 'exec_qty':
                                try:
                                    qty = prop_value['string']
                                except KeyError:
                                    qty = prop_value['int']
                        by_tester['report_types'].append(
                            {
                                "report_type": report_type,
                                "platform_id": int(platform_id),
                                "qty": int(qty)
                            }
                        )
                    by_testers.append(by_tester)
                setattr(self, 'by_tester', by_testers)
                setattr(self, 'with_tester', by_testers)

    def __repr__(self):
        """Show basic properties for this object

        Returns:
            str -- format text with values for
                'ReportBase: totals:dict={}'
        """
        return "RTPlanTotals: by_tester:dict={}".format(self.totals)
from lxml import etree


class LeadXml():

    def __init__(self, lead):
        self.lead = lead

    @staticmethod
    def create_referral_source_element(ref_type):
        ref_source = etree.Element("Field")
        ref_source.attrib['FieldId'] = '796'
        ref_source.attrib['FieldTitle'] = "Referral Source Type" 
        ref_source.attrib['FieldType'] = 'Dropdown'
        ref_source.attrib['Value'] = ref_type
        return ref_source

    def find_referral_type_element(self):
        rt = self.lead.xpath("//Field[@FieldTitle='Referral Source Type']")
        None if len(rt) == 0 else rt[0]

    def find_fields_element(self):
        return self.lead.xpath("//Fields")[0]

    def update_referral_type(self, ref_type):
        rt = self.find_referral_type_element()

        if rt is not None:
            rt.attrib['Value'] = ref_type
        else:
            rs = LeadXml.create_referral_source_element(ref_type)
            fields = self.find_fields_element()
            fields.append(rs)


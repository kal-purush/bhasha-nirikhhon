from jsonmodels import models, fields
from jsonmodels.validators import ValidationError


class PivotalResource(models.Base):
    kind = fields.StringField()
    # The type of this object: me. This field is read only

    def validate_on_post(self):
        for _, field in self:
            validators = getattr(field, 'post_validators', None)
            if validators:
                for val in validators:
                    val.validate()


class Instantiated(object):
    id = fields.IntField(required=True)
    created_at = fields.DateTimeField()
    # Creation time. This field is read only.
    updated_at = fields.DateTimeField()
    # Time of last update. This field is read only.

class TimeZone(PivotalResource):
    olson_name = fields.StringField()
    offset = fields.StringField()
    kind = fields.StringField()


class OneOf(object):
    """Ensure StringField value belongs to a given enum"""
    def __init__(self, *args):
        super(OneOf, self).__init__()
        self.args = args

    def validate(self, value):
        if value not in self.args:
            raise ValidationError("Value must be one of %s" %
                ", ".join(map(str, self.args)))


class RequiredOnPost(object):
    """Ensure value is present during a POST operation"""
    def validate(self, value):
        if value is None:
            raise ValidationError("Value required on POST operation.")

class PostValidators(object):
    def __init__(self, *args):
        self.validators = args

    def modify_schema(self, field_schema):
        print "added validators to %s" % field_schema
        field_schema['post_validators'] = self.validators

    def validate(self, value):
        pass



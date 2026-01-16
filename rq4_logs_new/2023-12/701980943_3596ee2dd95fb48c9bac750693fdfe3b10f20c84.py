from django.utils.translation import gettext_lazy as _
from rest_framework.serializers import MultipleChoiceField


class CustomMultipleChoiceField(MultipleChoiceField):
    """
    해당 필드는 MultipleChoiceField를 상속받아 중복되는 값 입력을 막고,
    최대 선택 가능한 항목의 개수를 제한할 수 있습니다.

    Args:
        max_choices (int, optional): 최대 선택 가능한 항목의 개수입니다. 기본값은 None입니다.
        그 외의 인자는 MultipleChoiceField를 상속받아 사용합니다.
    """

    default_error_messages = {
        "max_choices": _("Ensure this field has no more than {max_choices} items."),
        "duplicate_values": _("This list may not contain the same item twice."),
    }

    def __init__(self, **kwargs):
        self.max_choices = kwargs.pop("max_choices", None)
        super().__init__(**kwargs)

    def to_internal_value(self, data):
        if self.max_choices is not None:
            if len(data) > self.max_choices:
                self.fail("max_choices", max_choices=self.max_choices)

        if len(data) != len(set(data)):
            self.fail("duplicate_values")
        return super().to_internal_value(data)
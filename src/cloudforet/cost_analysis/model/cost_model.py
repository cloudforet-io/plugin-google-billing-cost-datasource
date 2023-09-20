from schematics.models import Model
from schematics.types import DictType, StringType, FloatType, DateTimeType

__all__ = ['Cost']


class Cost(Model):
    cost = FloatType(required=True)
    provider = StringType(required=True)
    region_code = StringType()
    product = StringType()
    usage_type = StringType()
    usage_unit = StringType(default=None)
    usage_quantity = FloatType(required=True)
    billed_date = DateTimeType(required=True)
    additional_info = DictType(StringType, default={})
    tags = DictType(StringType, default={})

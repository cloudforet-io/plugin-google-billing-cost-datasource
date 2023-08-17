from schematics.models import Model
from schematics.types import ListType, DateTimeType, StringType, DictType
from schematics.types.compound import ModelType

__all__ = ['Tasks']


class TaskOptions(Model):
    start = StringType(required=True)
    billing_dataset = StringType()
    sub_billing_account = StringType(default=None)
    target_project_id = StringType()


class Task(Model):
    task_options = ModelType(TaskOptions, required=True)


class Changed(Model):
    start = DateTimeType(required=True)
    end = DateTimeType(default=None)
    filter = DictType(StringType, default={})


class Tasks(Model):
    tasks = ListType(ModelType(Task), required=True)
    changed = ListType(ModelType(Changed), default=[])

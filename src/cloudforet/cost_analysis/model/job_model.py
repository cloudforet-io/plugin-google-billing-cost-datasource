from schematics.models import Model
from schematics.types import ListType, DateTimeType, StringType, DictType
from schematics.types.compound import ModelType

__all__ = ['Tasks']


class TaskOptions(Model):
    start = StringType(required=True, max_length=7)
    billing_dataset = StringType()
    billing_account_id = StringType(default=None)
    project_id = StringType()


class Task(Model):
    task_options = ModelType(TaskOptions, required=True)


class Changed(Model):
    start = DateTimeType(required=True, max_length=7)
    end = DateTimeType(default=None, max_length=7)
    filter = DictType(StringType, default={})


class Tasks(Model):
    tasks = ListType(ModelType(Task), required=True)
    changed = ListType(ModelType(Changed), default=[])

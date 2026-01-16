from functools import wraps


def once(f):
    """
    Run func only once
    The running can be reset by setting 'has_run' attribute to False
    """

    @wraps(f)
    def wrapper(*args, **kwargs):
        if not wrapper.has_run:
            result = f(*args, **kwargs)
            wrapper.has_run = True
            return result

    wrapper.has_run = True
    return wrapper


@once
def get_logger():
    return [1, 2, 3] * 2


assert id(get_logger()) == id(get_logger()), "Not equal"
print("SUCCESS!")


class Descriptor:
    def __init__(self, name=None, default=None):
        self.name = name
        self.default = default

    def __set__(self, instance, value):
        instance.__dict__[self.name] = value

    def __get__(self, instance, objtype):
        if self.name not in instance.__dict__:
            instance.__dict__[self.name] = self.default
        return instance.__dict__[self.name]

    def __delete__(self, instance):
        raise AttributeError("Can't delete")


class Typed(Descriptor):
    type_ = object
    extra_methods = []

    def __set__(self, instance, value):
        if not isinstance(value, self.type_):
            raise TypeError('Expected %s' % self.type_)
        super().__set__(instance, value)


# Specialized types
class Numeric(Typed):
    extra_methods = ['gt', 'gte']

    def gt(instance_value, value):
        return instance_value > value

    def gte(instance_value, value):
        return instance_value >= value


class Integer(Numeric):
    type_ = int


class ModelMeta(type):
    def __new__(metacls, clsname, bases=None, clsdict=None):
        cls = super().__new__(metacls, clsname, bases, clsdict)
        extra_attrs = []
        for attr_name, attr_value in cls.__dict__.items():
            if isinstance(attr_value, Typed):
                extra_attrs += [
                    (attr_name, extra_method, getattr(attr_value.__class__, extra_method))
                    for extra_method in attr_value.extra_methods
                ]

        for attr, extra, func in extra_attrs:
            setattr(
                cls,
                f'{attr}__{extra}',
                lambda self, value, attr=attr, func=func: func(getattr(self, attr), value)
            )

        return cls


class Range(Descriptor):
    def __init__(self, name, max_value, min_value):
        super().__init__()
        self.name = name
        self.min_value = min_value
        self.max_value = max_value

    def __set__(self, instance, value):
        if self.min_value > value > self.max_value:
            print('Invalid value')
        super().__set__(instance, value)


class RangeInteger(Range, Integer):
    pass


class Employee(metaclass=ModelMeta):
    kpi_score = RangeInteger(name='kpi_score', min_value=0, max_value=100)


emp = Employee()
emp.kpi_score = 15
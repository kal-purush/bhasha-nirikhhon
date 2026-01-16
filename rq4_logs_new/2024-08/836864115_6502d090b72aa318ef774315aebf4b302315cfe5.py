from django_filters import FilterSet
from django_filters.filters import (NumberFilter, ModelMultipleChoiceFilter)
from products.models import (Notebook, Smartphone, Watch, Headphones, Color,
                             Brand, Processor, Ram, Display, OperatingSystem,
                             SpecificWatch, SpecificHeadphones, ShopingCart)


# def is_in_shopping_cart_filter(self, queryset, name, value):
#     if value and self.request.user.is_authenticated:
#         return queryset.filter(shopping_cart__user=self.request.user)
#     return queryset


class NotebookFilter(FilterSet):
    """тут можно добавить кэштрование, для уменьшения запросов к базе"""

    brand_name = ModelMultipleChoiceFilter(
        field_name="brand__name",
        to_field_name="name",
        queryset=Brand.objects.all())
    min_price = NumberFilter(
        field_name="price_after_discount",
        lookup_expr='gte')
    max_price = NumberFilter(
        field_name="price_after_discount",
        lookup_expr='lte')
    processor_name = ModelMultipleChoiceFilter(
        field_name="processor__processor_name",
        to_field_name='processor_name',
        queryset=Processor.objects.values('processor_name').distinct())
    number_of_cores = ModelMultipleChoiceFilter(
        field_name="processor__number_of_cores",
        to_field_name='number_of_cores',
        queryset=Processor.objects.values('number_of_cores').distinct())
    ram_value = ModelMultipleChoiceFilter(
        field_name="ram__ram_memory",
        to_field_name='ram_memory',
        queryset=Ram.objects.values('ram_memory').distinct())
    diagonal_value = ModelMultipleChoiceFilter(
        field_name="display__diagonal",
        to_field_name='diagonal',
        queryset=Display.objects.values('diagonal').distinct())
    operating_system_name = ModelMultipleChoiceFilter(
        field_name="operating_system__operating_system",
        to_field_name='operating_system',
        queryset=OperatingSystem.objects.values('operating_system').distinct())

    class Meta:
        model = Notebook
        fields = ['brand_name',
                  'min_price',
                  'max_price',
                  'processor_name',
                  'number_of_cores',
                  'ram_value',
                  'diagonal_value',
                  'operating_system_name']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.filters['brand_name'].label = 'Бренд'
        self.filters['min_price'].label = 'Минимальная цена'
        self.filters['max_price'].label = 'Максимальная цена'
        self.filters['processor_name'].label = 'Процессор'
        self.filters['number_of_cores'].label = 'Количество ядер'
        self.filters['ram_value'].label = 'Объем оперативной памяти'
        self.filters['diagonal_value'].label = 'Диагональ экрана'
        self.filters['operating_system_name'].label = 'Операционная система'


class SmartphoneFilter(FilterSet):
    """тут можно добавить кэштрование, для уменьшения запросов к базе"""
    name = ModelMultipleChoiceFilter(
        field_name="name",
        to_field_name="name",
        queryset=Smartphone.objects.values('name').distinct())

    brand_name = ModelMultipleChoiceFilter(
        field_name="brand__name",
        to_field_name="name",
        queryset=Brand.objects.all())
    color = ModelMultipleChoiceFilter(
        field_name="color__color_name",
        to_field_name="color_name",
        queryset=Color.objects.values('color_name').distinct())
    min_price = NumberFilter(
        field_name="price_after_discount",
        lookup_expr='gte')
    max_price = NumberFilter(
        field_name="price_after_discount",
        lookup_expr='lte')
    ram_memory = ModelMultipleChoiceFilter(
        field_name="ram__ram_memory",
        to_field_name="ram_memory",
        queryset=Ram.objects.values('ram_memory').distinct())

    class Meta:
        model = Smartphone
        fields = ['name',
                  'brand_name',
                  'color',
                  'min_price',
                  'max_price',
                  'ram_memory']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.filters['name'].label = 'Название'
        self.filters['brand_name'].label = 'Бренд'
        self.filters['color'].label = 'Цвет'
        self.filters['min_price'].label = 'Минимальная цена'
        self.filters['max_price'].label = 'Максимальная цена'
        self.filters['ram_memory'].label = 'Объем оперативной памяти'


class WatchFilter(FilterSet):
    """тут можно добавить кэштрование, для уменьшения запросов к базе"""
    brand_name = ModelMultipleChoiceFilter(
        field_name="brand__name",
        to_field_name="name",
        queryset=Brand.objects.all())
    color = ModelMultipleChoiceFilter(
        field_name="color__color_name",
        to_field_name="color_name",
        queryset=Color.objects.values('color_name').distinct())
    min_price = NumberFilter(
        field_name="price_after_discount",
        lookup_expr='gte')
    max_price = NumberFilter(
        field_name="price_after_discount",
        lookup_expr='lte')
    diagonal = ModelMultipleChoiceFilter(
        field_name="display__diagonal",
        to_field_name="diagonal",
        queryset=Display.objects.values('diagonal').distinct())
    typ_of_display = ModelMultipleChoiceFilter(
        field_name="display__typ_of_display",
        to_field_name="typ_of_display",
        queryset=Display.objects.values('typ_of_display').distinct())
    watertightness = ModelMultipleChoiceFilter(
        field_name="specificwatch__watertightness",
        to_field_name="watertightness",
        queryset=SpecificWatch.objects.values('watertightness').distinct())

    class Meta:
        model = Watch
        fields = ['brand_name',
                  'color',
                  'min_price',
                  'max_price',
                  'diagonal',
                  'typ_of_display',
                  'watertightness']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.filters['brand_name'].label = 'Бренд'
        self.filters['color'].label = 'Цвет'
        self.filters['min_price'].label = 'Минимальная цена'
        self.filters['max_price'].label = 'Максимальная цена'
        self.filters['diagonal'].label = 'Диагональ экрана'
        self.filters['typ_of_display'].label = 'Тип экрана'
        self.filters['watertightness'].label = 'Водонепроницаемость'


class HeadphonesFilter(FilterSet):
    """тут можно добавить кэштрование, для уменьшения запросов к базе"""
    brand_name = ModelMultipleChoiceFilter(
        field_name="brand__name",
        to_field_name="name",
        queryset=Brand.objects.all())
    color = ModelMultipleChoiceFilter(
        field_name="color__color_name",
        to_field_name="color_name",
        queryset=Color.objects.values('color_name').distinct())
    min_price = NumberFilter(
        field_name="price_after_discount",
        lookup_expr='gte')
    max_price = NumberFilter(
        field_name="price_after_discount",
        lookup_expr='lte')
    typ_construction = ModelMultipleChoiceFilter(
        field_name="specificheadphones__typ_construction",
        to_field_name="typ_construction",
        queryset=SpecificHeadphones.objects.values('typ_construction').distinct())
    signal_method = ModelMultipleChoiceFilter(
        field_name="specificheadphones__signal_method",
        to_field_name="signal_method",
        queryset=SpecificHeadphones.objects.values('signal_method').distinct())
    sound_circuit_format = ModelMultipleChoiceFilter(
        field_name="specificheadphones__sound_circuit_format",
        to_field_name="sound_circuit_format",
        queryset=SpecificHeadphones.objects.values('sound_circuit_format').distinct())

    class Meta:
        model = Headphones
        fields = ['brand_name',
                  'color',
                  'min_price',
                  'max_price',
                  'typ_construction',
                  'signal_method',
                  'sound_circuit_format']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.filters['brand_name'].label = 'Бренд'
        self.filters['color'].label = 'Цвет'
        self.filters['min_price'].label = 'Минимальная цена'
        self.filters['max_price'].label = 'Максимальная цена'
        self.filters['typ_construction'].label = 'Тип конструкции'
        self.filters['signal_method'].label = 'Способ передачи сигнала'
        self.filters['sound_circuit_format'].label = 'Формат кабеля'
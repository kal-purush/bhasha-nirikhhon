import django_filters
from api.models import Property, Feature
from rest_framework import filters

class PropertyFilter(filters.FilterSet):
    """Filters used for the Property model.

    Filters are added to a request as query parameters. For example the follwoing
    request will filter based on the minimum hosue price.

    `/api/property?min_price=100000`

    while not an explicit attribute of this class the `features` property can
    also be filtered on by suppluing a list of the feature ids. For example:

    `/api/property?features=1`
    

    Attributes:
        min_price: Supplying this filter returns only properties with a price
            greater than or equal to the supplied value.

        max_price: Supplying this filter returns only properties with a price
            less than or equal to the supplied value.
        
        min_bedrooms: Supplying this filter returns only properties which have
            a number of bedrooms greater than or equal to the supplied value.
        
        max_bedrooms: Supplying this filter returns only properties which have
            a number of bedrooms less than or equal to the supplied value.
        
        min_bathrooms: Supplying this filter returns only properties which have
            a number of bathrooms greater than or equal to the supplied value.
        
        max_bathrooms: Supplying this filter returns only properties which have
            a number of bathrooms less than or equal to the supplied value.
        
        min_car_spaces: Supplying this filter returns only properties which have
            a number of car spaces greater than or equal to the supplied value.
        
        max_car_spaces: Supplying this filter returns only properties which have
            a number of car_spaces less than or equal to the supplied value.
        
        address_contains: Supplying this filter will return only properties which
            contain the supplied text in their geocded address.
        
    """

    min_price = django_filters.NumberFilter(name="price", lookup_type="gte")
    max_price = django_filters.NumberFilter(name="price", lookup_type="lte")

    min_bedrooms = django_filters.NumberFilter(name="bedrooms", lookup_type="gte")
    max_bedrooms = django_filters.NumberFilter(name="bedrooms", lookup_type="lte")

    min_bathrooms = django_filters.NumberFilter(name="bathrooms", lookup_type="gte")
    max_bathrooms = django_filters.NumberFilter(name="bathrooms", lookup_type="lte")

    min_car_spaces = django_filters.NumberFilter(name="car_spaces", lookup_type="gte")
    max_car_spaces = django_filters.NumberFilter(name="car_spaces", lookup_type="lte")

    address_contains = django_filters.CharFilter(name="geocoded_address", lookup_type="contains")

    class Meta:
        model = Property
        fields = ["min_price", "max_price", "min_bedrooms", "max_bedrooms", 
                "min_bathrooms", "max_bathrooms", "min_car_spaces", "max_car_spaces",
                "features", "address_contains"]



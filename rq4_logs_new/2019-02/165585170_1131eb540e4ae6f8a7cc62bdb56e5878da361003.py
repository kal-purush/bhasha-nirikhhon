from rest_framework import serializers
from .models import Ticket, Passenger
from flight.models import Flight
from user.serializers import UserSerializer
from flight.serializers import FlightSerializer
from flighty.validators.choice_validators import choice_validator


class PassengerSerializer(serializers.ModelSerializer):

    name  = serializers.CharField(max_length = 100)
    email = serializers.EmailField(allow_blank=True)
    nationality = serializers.CharField(max_length=100)
    passport_no = serializers.CharField(max_length=100)
    expiration_date = serializers.DateField()
    issuarance_country = serializers.CharField(max_length=100)
    telephone = serializers.CharField(max_length=100)

    class Meta:
        model = Passenger
        fields = (
            'name',
            'email',
            'nationality',
            'passport_no',
            'expiration_date',
            'issuarance_country',
            'telephone'
        )

class TicketSerializer(serializers.ModelSerializer):
    """Handles serialization and deserialization of Flight objects."""

    user = UserSerializer(read_only=True)
    flight = FlightSerializer(read_only=True)
    passengers = PassengerSerializer(many=True)
    is_ticketed = serializers.BooleanField(read_only=True)
    is_valid = serializers.BooleanField(read_only=True)
    ticket_no = serializers.IntegerField(read_only=True)
    flight_id = serializers.IntegerField(write_only=True)
    form_of_payment = serializers.CharField(max_length = 100, validators=[choice_validator(Ticket.PAYMENT_CHOICES)])
    valid_till = serializers.DateTimeField(read_only=True)
    total_fare  = serializers.FloatField(read_only=True)
    number_of_travellers = serializers.IntegerField(read_only=True)

    class Meta:
        model = Ticket
        fields = (
            'id',
            'flight_id',
            'flight',
            'is_ticketed',
            'is_valid',
            'ticket_no',
            'form_of_payment',
            'valid_till',
            'total_fare',
            'number_of_travellers',
            'passengers',
            'user',
            'is_ticketed'
        )

        read_only_fields = ('id', 'total_fare')
    
    def validate(self, data):
        # import pdb; pdb.set_trace()
        flight = Flight.objects.get(pk=data['flight_id'])
        booked_tickets = Flight.objects.filter(ticket__is_ticketed=True).count()
        available_space = flight.travellers_capacity - booked_tickets
        number_of_travellers = len(data['passengers'])
        if flight and number_of_travellers > available_space:
            raise serializers.ValidationError('Number of passengers is greater than the available seat')
        else:
            data['number_of_travellers'] = number_of_travellers
        return data

    def update(self, instance, validated_data):
        """Performs an update on a User."""
        passengers = validated_data.pop('passengers', [])

        for (key, value) in validated_data.items():
            # For the keys remaining in `validated_data`, we will set them on
            # the current `User` instance one at a time.
            setattr(instance, key, value)

        # After everything has been updated we must explicitly save
        # the model. It's worth pointing out that `.set_password()` does not
        # save the model.
        instance.save()

        for passenger in passengers:
            passenger_instance = Ticket.filter(passenger__pk=passenger['id'])
            if passenger:
                for (key, value) in passenger.items():
                    setattr(passenger_instance, key, value)
                passenger_instance.save()

        return instance
    
    def create(self):
        """Performs an update on a User."""
        user_id = self.context['user'].id
        passengers = self.validated_data.pop('passengers', [])
        self.validated_data['user_id'] = user_id
        ticket, created = Ticket.objects.get_or_create(**self.validated_data)
        self.validated_data['passengers'] = passengers
        passenger_instances = []
        if created:
            for passenger in passengers:
                passenger['ticket'] = ticket
                passenger_instances.append(Passenger(**passenger))
            Passenger.objects.bulk_create(passenger_instances)
        return ticket
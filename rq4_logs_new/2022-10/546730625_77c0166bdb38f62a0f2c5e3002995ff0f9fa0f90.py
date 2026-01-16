from rest_framework import serializers
from .models import Person, Team, SHIRT_SIZES, MONTHS


class TeamSerializer(serializers.Serializer):

    id = serializers.IntegerField(read_only=True)
    name = serializers.CharField(required=True)
    country = serializers.CharField(required=True)

    def create(self, validated_data):
        return Team.objects.create(**validated_data)

    def update(self, instance, validated_data):
        instance.name = validated_data.get('name', instance.name)
        instance.country = validated_data.get('country', instance.country)
        instance.save()
        return instance


class PersonSerializer(serializers.Serializer):

    # pole tylko do odczytu, tutaj dla id działa też autoincrement
    id = serializers.IntegerField(read_only=True)
    # pole wymagane
    name = serializers.CharField(required=True)
    # pole mapowane z klasy modelu, z podaniem wartości domyślnych
    # zwróć uwagę na zapisywaną wartość do bazy dla default={wybór}[0] oraz default={wybór}[0][0]
    # w pliku models.py SHIRT_SIZES oraz MONTHS zostały wyniesione jako stałe do poziomu zmiennych skryptu
    # (nie wewnątrz modelu)
    shirt_size = serializers.ChoiceField(choices=SHIRT_SIZES, default=SHIRT_SIZES[0][0])
    month_added = serializers.ChoiceField(choices=MONTHS, default=MONTHS[0][0], allow_null=True)
    # odzwierciedlenie pola w postaci klucza obcego
    # przy dodawaniu nowego obiektu możemy odwołać się do istniejącego poprzez inicjalizację nowego obiektu
    # np. team=Team({id}) lub wcześniejszym stworzeniu nowej instancji tej klasy
    team = serializers.PrimaryKeyRelatedField(queryset=Team.objects.all(), allow_null=True)

    def create(self, validated_data):
        return Person.objects.create(**validated_data)

    def update(self, instance, validated_data):
        instance.name = validated_data.get('name', instance.name)
        instance.shirt_size = validated_data.get('shirt_size', instance.shirt_size)
        instance.month_added = validated_data.get('month_added', instance.month_added)
        instance.team = validated_data.get('team', instance.team)
        instance.save()
        return instance
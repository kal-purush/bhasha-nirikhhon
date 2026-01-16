from rest_framework import serializers
from management.models import *
from entrylog.models import *
from facilities.models import *

class StudentSerializers(serializers.ModelSerializer):
    hostel_name = serializers.CharField(source='hostel_name.hostel_name')

    class Meta:
        model = student_db
        fields = '__all__'
    
    def create(self, validated_data):
        """
        Create and return a new `student_db` instance, given the validated data.
        """
        print(validated_data)
        try:
            hostel_fk = hostel_db.objects.get(hostel_name=validated_data['hostel_name']['hostel_name'])
            validated_data['hostel_name'] = hostel_fk
            return student_db.objects.create(**validated_data)
        except:
            raise serializers.ValidationError("Hostel does not exist")

class PasswordSerializers(serializers.ModelSerializer):
    class Meta:
        model = password_db
        fields = '__all__'

class WardenSerializers(serializers.ModelSerializer):
    class Meta:
        model = warden_db
        fields = '__all__'

class HostelSerializers(serializers.ModelSerializer):
    class Meta:
        model = hostel_db
        fields = '__all__'

class EntryLogSerializers(serializers.ModelSerializer):
    class Meta:
        model = EntryLog
        fields = '__all__'

class GuardDetailSerializers(serializers.ModelSerializer):
    class Meta:
        model = Guard_Detail
        fields = '__all__'

class DefaulterSerializers(serializers.ModelSerializer):
    class Meta:
        model = Defaulters
        fields = '__all__'

class ComplaintSerializers(serializers.ModelSerializer):
    class Meta:
        model = complaints_db
        fields = '__all__'

class LeaveApplicationSerializers(serializers.ModelSerializer):
    class Meta:
        model = leave_application_db
        fields = '__all__'
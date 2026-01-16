from rest_framework import serializers
from .models import Department, Managers


class DepartmentSerializer(serializers.ModelSerializer):
    class Meta:
        model = Department
        fields = '__all__'

class ManagersSerializer(serializers.ModelSerializer):
    class Meta:
        model = Managers
        fields = '__all__'
        depth = 1
        extra_kwargs = {
            'department': {'write_only': True}
        }
        read_only_fields = ('department',)
        write_only_fields = ('department',)

    def create(self, validated_data):
        department = validated_data.pop('department')
        department_instance = Department.objects.create(**department)
        validated_data['department'] = department_instance
        return Managers.objects.create(**validated_data)
    
    def update(self, instance, validated_data):
        department = validated_data.pop('department')
        department_instance = Department.objects.create(**department)
        validated_data['department'] = department_instance
        instance.department = department_instance
        return super().update(instance, validated_data)
    
    def validate_department(self, value):
        if value.get('name') == 'IT':
            raise serializers.ValidationError('IT department is not allowed')
        return value
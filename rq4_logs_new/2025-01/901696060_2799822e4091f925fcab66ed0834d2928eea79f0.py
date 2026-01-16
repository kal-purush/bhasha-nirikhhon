from rest_framework import serializers

class SGPAInputSerializer(serializers.Serializer):
    sgpa = serializers.FloatField(error_messages={
            'required': 'SGPA is required.',
            'invalid': 'SGPA must be a number.',
        })
    credits = serializers.IntegerField(error_messages={
            'required': 'Credits are required.',
            'invalid': 'Credits must be an integer.',
        })

    def validate_sgpa(self,value):
        if value < 0 or value > 10:
           raise serializers.ValidationError("SGPA must be between 0 and 10.")
        return  value
    
    def validate_credits(self,value):
        if value <= 0:
            raise serializers.ValidationError("Credits must be a positive integer.")
        return value
    
    def validate(self, data):
        if data['sgpa'] < 4 and data['credits'] > 30:
            raise serializers.ValidationError("Low SGPA with high credits seems invalid.")
        return data


class CGPACalculatorSerializer(serializers.Serializer):
    semesters = serializers.ListField(child=SGPAInputSerializer(),allow_empty=False,error_messages={
        'required': "The 'semesters' field is required.",
        'blank': "The 'semesters' field cannot be blank.",
    },
    )


from rest_framework import serializers

class CompletedSemesterSerializer(serializers.Serializer):
    sgpa = serializers.FloatField()
    credits = serializers.IntegerField()

class RequiredSGPASerializer(serializers.Serializer):
    completed_semesters = serializers.ListField(child=CompletedSemesterSerializer())
    total_program_credits = serializers.IntegerField()
    future_semester_credits = serializers.IntegerField()
    expected_cgpa = serializers.FloatField()
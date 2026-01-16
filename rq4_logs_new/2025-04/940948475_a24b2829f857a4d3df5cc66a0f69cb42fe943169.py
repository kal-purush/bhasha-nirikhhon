from rest_framework.response import Response
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework import status
from ..serializers import TeacherSerializer
from ..models import Teacher
from ..logics.email import send_email_sync
from django.contrib.auth import get_user_model
User = get_user_model()
import os, datetime


@api_view(['GET'])
# @permission_classes([IsAuthenticated])
def get_all_class_teachers(request):
    teachers = Teacher.objects.filter(type="class_teacher")
    serializer = TeacherSerializer(teachers, many=True)
    return Response(serializer.data)


# 2. Add a Class Teacher
@api_view(['POST'])
# @permission_classes([IsAuthenticated])
def add_class_teacher(request):
    serializer = TeacherSerializer(data=request.data)
    if serializer.is_valid():
        teacher_data = serializer.validated_data
        teacher_data["name"] = teacher_data["name"].title()
        username = teacher_data["name"].lower().replace(" ", "")
        email = teacher_data["email"].lower().strip()
        
        if User.objects.filter(email=email).exists():
            return Response({"error": "User with this email already exists"}, status=status.HTTP_400_BAD_REQUEST)
        
        user = User.objects.create(username=username, email=email)
        
        teacher = Teacher.objects.create(
            user=user,
            name=teacher_data["name"],
            type="class_teacher",
            phone=teacher_data["phone"],
            email=teacher_data["email"],
            date_of_birth=teacher_data["date_of_birth"],
            school=teacher_data["school"],
            school_id=teacher_data["school_id"],
            address=teacher_data["address"],
            city=teacher_data["city"],
            state=teacher_data["state"],
            pincode=teacher_data["pincode"],
            profile_image=teacher_data.get("profile_image"),
            class_assigned=teacher_data.get("class_assigned")  # ⚡ Important
        )
        
        year = teacher.date_of_birth.year
        password = f"{teacher.name.title().split(' ')[0]}@{year}{teacher.id}"
        user.set_password(password)
        user.save()

        email_subject = "EduMet Account Login Credentials"
        email_context = {
            'name': teacher.name,
            'email': teacher.email,
            'username': username,
            'password': password,
            'school': teacher.school,
            'current_year': datetime.datetime.now().year
        }

        send_email_sync(
            subject=email_subject,
            template_name='emails/welcome_email.html',
            context=email_context,
            recipient_email=email
        )

        return Response({
            "message": "Class Teacher credentials sent to their mail address!",
            "data": TeacherSerializer(teacher).data
        }, status=status.HTTP_201_CREATED)

    return Response({
        "message": "Failed to add class teacher.",
        "errors": serializer.errors
    }, status=status.HTTP_400_BAD_REQUEST)


# 3. View Class Teacher by ID
@api_view(['GET'])
# @permission_classes([IsAuthenticated])
def view_class_teacher(request, pk):
    try:
        teacher = Teacher.objects.get(pk=pk, type="class_teacher")
    except Teacher.DoesNotExist:
        return Response({"error": "Class Teacher not found"}, status=status.HTTP_404_NOT_FOUND)
    
    serializer = TeacherSerializer(teacher)
    return Response(serializer.data)


# 4. Update Class Teacher by ID
@api_view(['PUT'])
# @permission_classes([IsAuthenticated])
def update_class_teacher(request, pk):
    try:
        teacher = Teacher.objects.get(pk=pk, type="class_teacher")
    except Teacher.DoesNotExist:
        return Response({"error": "Class Teacher not found"}, status=status.HTTP_404_NOT_FOUND)
    
    serializer = TeacherSerializer(teacher, data=request.data)
    if serializer.is_valid():
        serializer.save()
        user = teacher.user
        if "name" in serializer.validated_data:
            user.username = serializer.validated_data["name"].lower().replace(" ", "")
        if "email" in serializer.validated_data:
            user.email = serializer.validated_data["email"]
        user.save()
        return Response({"message": "Class Teacher updated successfully!"}, status=status.HTTP_200_OK)

    return Response({
        "message": "Failed to update class teacher.",
        "errors": serializer.errors
    }, status=status.HTTP_400_BAD_REQUEST)


# 5. Delete Class Teacher by ID
@api_view(['DELETE'])
# @permission_classes([IsAuthenticated])
def delete_class_teacher(request, pk):
    try:
        teacher = Teacher.objects.get(pk=pk, type="class_teacher")
    except Teacher.DoesNotExist:
        return Response({"error": "Class Teacher not found"}, status=status.HTTP_404_NOT_FOUND)
    
    if teacher.profile_image and os.path.isfile(teacher.profile_image.path):
        os.remove(teacher.profile_image.path)

    teacher.user.delete()

    email_subject = "Goodbye from EduMet - Account Has Been Permanently Removed"
    email_context = {
        'name': teacher.name,
        'email': teacher.email,
        'current_year': datetime.datetime.now().year
    }

    send_email_sync(
        subject=email_subject,
        template_name='emails/delete_teacher.html',
        context=email_context,
        recipient_email=teacher.email
    )

    return Response({"message": "Class Teacher deleted successfully"}, status=status.HTTP_200_OK)
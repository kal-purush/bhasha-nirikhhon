from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from .models import Rank
from .serializers import RankSerializer
from django.contrib.auth import get_user_model

User = get_user_model()

class RankView(APIView):
    def post(self, request):
        user_id = request.data.get('user_id')
        score = request.data.get('score')

        if not user_id or not score:
            return Response(status=status.HTTP_400_BAD_REQUEST)

        user = User.objects.filter(id=user_id).first()
        if not user:
            return Response({'message': '존재하지 않는 유저 입니다'},status=status.HTTP_404_NOT_FOUND)

        # 해당 유저의 기존 랭킹 정보를 가져오거나 없다면 생성
        rank = Rank.objects.filter(user_id=user.id).first()
        if not rank:
            rank = Rank(user_id=user)

        # 점수가 클 때만 업데이트한다
        if rank.score is None or score > rank.score:
            rank.score = score
            rank.save()

            # 업데이트
            ranks = Rank.objects.all().order_by('-score', 'created_at')
            rank_list = list(ranks)
            for i, r in enumerate(rank_list):
                if r.user_id == user:
                    r.rank = i + 1
                    r.save()

        serializer = RankSerializer(rank)
        return Response(serializer.data, status=status.HTTP_201_CREATED)
    def get(self, request):
        ranks = Rank.objects.all().order_by('rank')
        serializer = RankSerializer(ranks, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)
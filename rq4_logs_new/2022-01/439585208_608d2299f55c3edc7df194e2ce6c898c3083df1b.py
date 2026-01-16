from django.test import TestCase
from django.urls import reverse
from django.contrib.auth.models import User
from rest_framework.authtoken.models import Token
from rest_framework.test import APIRequestFactory
from rest_framework.test import APITestCase
from rest_framework.test import force_authenticate
from ..views import JobViewSet
import json

class AuthJobViewTests(APITestCase):

    """authorizedでGETメソッドでJobViewTestsにアクセスしてステータスコード200を返されることを確認"""
    # ライフサイクルの初めにuser, tokenの作成を行う
    def setUp(self):
        # APIRequestFactoryのインスタンスを作成
        self.factory = APIRequestFactory()
        # Userのオブジェクトを作成、testDBに保存し、self.userへ代入
        self.user = User.objects.create_user(
            username='test', email='test@test.com', password='password')
        # self.userのログイン情報からtokenのオブジェクトを作成、
        # testDBに保存し、self.userへ代入
        self.token = Token.objects.create(user=self.user)
    
    # setUp後に実行される
    def test_token_auth(self):
        # JobViewSetにtokenをつけて、GETメソッド
        request = self.factory.get('tinder:jobs-list', HTTP_AUTHORIZATION='Token {}'.format(self.token))
        # requestにuserの認証情報を追加し、認証済みrequestにする
        force_authenticate(request, user=self.user)
        # as_viewはMemberViewSetのインスタンス(job_view)を生成するようなもの
        job_view = JobViewSet.as_view({'get': 'list'})
        # 認証済みのリクエストをJobViewSetに送る
        response = job_view(request)
        # JobViewSetのレスポンスが200であることを確認
        self.assertEqual(response.status_code, 200)

class UnauthJobViewTests(TestCase):
  """JobViewSetのテストクラス"""
  def test_unauth_get(self):
      """unauthorizedでGETメソッドでJobViewTestsにアクセスしてステータスコード401を返されることを確認"""
      # tinder_appの'jobs-list'というurlのname(自動で決まる)にGETメソッド
      response = self.client.get(reverse('tinder:jobs-list'))
      # レスポンスのステータスコードが401になっているか。
      self.assertEqual(response.status_code, 401)
import json
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt

@csrf_exempt
def diagnose(request):
    if request.method == 'POST':
        # JSON文字列
        datas = json.loads(request.body)

        print("--受取り値--------------------------")
        print(type(datas))
        print(datas)

        # requestには、param1,param2の変数がpostされたものとする
        ret = {"data": "param1:" + datas["param1"] + ", param2:" + datas["param2"]}

        # JSONに変換して戻す
        return JsonResponse(ret)
    
    else:
        return HttpResponse("ここをどこだと思っているんだあああ")
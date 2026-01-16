from fastapi import APIRouter
from common.read_yaml import readYamlHandler
from dao.elasticsearch.elasticsearch_base import ElasticSearchBase

updateRouter = APIRouter()
esIndexName = readYamlHandler.read_conf()['elasticsearch']['esIndexName']
es = ElasticSearchBase()


@updateRouter.put("/dashboard/update/clicks/{doc_id}")
async def updateClicks(doc_id: str):
    '''
    当点击经验详情,为经验的点击量+1
    '''
    # print("doc_id is:", doc_id)
    body = {
        "script": {
            "lang": "painless",
            "source": """DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX"); df.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai")); Date date = new Date(); ctx._source.updateTime = df.format(date); ctx._source.clicks += 1"""
        }
    }
    es.UpdateDocs(index=esIndexName, id=doc_id, body=body)
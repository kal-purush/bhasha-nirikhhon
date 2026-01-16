from wit import Wit
import wolframalpha as wolf
from louie.yelpfusion import YelpFusion

__all__ = ['wolfram_search', 'witclient', 'wolfclient', 'yelpclient']

WIT_TOKEN = 'FCLEILUP6T2PTIH6TSWWJYFJYKG3KL2L'

FB_PAGE_TOKEN = 'EAAVWYdbX2BUBAJZBmlbIeZCoocO5CdRHY82VNs8drNbB0yNL5bj63K0ZCQqIqzAbrl0u2ollXrsFIiRMfebWAQmpF1sw2EsThg1TpDulsygqGkQQ7dcHZCZB6W6QGlejXKYEg0ObqZAOTXGqKe9exLf57ZCQW546Kh5W66lEOvaGjX3ffruHXXT'
FB_VERIFY_TOKEN = 'hello'
WOLFRAM_TOKEN = '64J9LH-5Q8357GKRK'
GOOGS_PLACES_TOKEN = 'AIzaSyABRaPH0tzxRT_sVBkkGr5zWkbN3y7jN9Q'
YELP_APP_ID = 'xqTzjWmr8PIUqv9gkQLWBw'
YELP_CLIENT_SECRET = '4YtXv1OVhISGWM4Eb1ji7YJc0igSEAOkvXXyiH3KA0tNKZmGhUoh9M2VAlexfIST'

witclient = Wit(access_token=WIT_TOKEN)
wolfclient = wolf.Client(WOLFRAM_TOKEN)
yelpclient = YelpFusion(YELP_APP_ID, YELP_CLIENT_SECRET)

def wolfram_search(simple_question):
    query_result = wolfclient.query(str(simple_question))
    sub_pod_num = 0

    if(query_result['@success'] == 'false'):
        return 'We did not find an answer for your question.'

    elif(list(query_result.results)):
         return next(query_result.results).text

    else:
        for pod in query_result.pods:
            for sub in pod.subpods:
                sub_pod_num += 1
                if(sub_pod_num == 6):
                    return sub['img']['@title']

def yelp_search(*args, **kwargs):
    result = yelpclientself.search(*args, **kwargs)
    top_option = result
    pass
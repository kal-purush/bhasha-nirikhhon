# From https://pypi.org/project/python-graphql-client/
from python_graphql_client import GraphqlClient

# Instantiate the client with an endpoint.
client = GraphqlClient(endpoint="https://leetcode.com/graphql")

# Create the query string and variables required for the request.

{"query":"query problemsetQuestionList($categorySlug: String, $limit: Int, $skip: Int, $filters: QuestionListFilterInput) {
    problemsetQuestionList: questionList( categorySlug: $categorySlug limit: $limit skip: $skip filters: $filters) {
        total: totalNum
        questions: data{
            acRate
            difficulty
            freqBar
            frontendQuestionId:
            questionFrontendId
            isFavor
            paidOnly: isPaidOnly
            status
            title
            titleSlug
            topicTags {
                name
                id
                slug
            }
            hasSolution
            hasVideoSolution
        }
    }
}","variables":{"categorySlug":"","skip":0,"limit":50,"filters":{}}}'


query = """
query problemsetQuestionList($categorySlug: String, $limit: Int, $skip: Int, $filters: QuestionListFilterInput) {
    problemsetQuestionList: questionList(
        categorySlug: $categorySlug
        limit: $limit
        skip: $skip
        filters: $filters
    ) {
        total: totalNum
        questions: data {
            # acRate
            difficulty
            # freqBar
            frontendQuestionId: questionFrontendId
            # isFavor
            # paidOnly: isPaidOnly
            # status
            title
            # titleSlug
            # topicTags {
            #     name
            #     id
            #     slug
            # }
            # hasSolution
            # hasVideoSolution
        }
    }
}
"""
# variables = {"countryCode": "CA"}
variables = {"categorySlug": "", "skip": 0, "limit": 1, "filters": {"questions.difficulty": "easy"}}
# variables = {"categorySlug": "", "skip": 0, "limit": 1, "filters": {}}
# variables = {"titleSlug": "two-sum"}

query2 = """ 
query learningContext($currentQuestionSlug: String!, $categorySlug: String, $envId: String, $envType: String, $filters: QuestionListFilterInput) {
  learningContextV2(
    currentQuestionSlug: $currentQuestionSlug
    categorySlug: $categorySlug
    envId: $envId
    envType: $envType
    filters: $filters
  ) {
    name
    backLink
    nextQuestion {
      difficulty
      title
      titleSlug
      questionFrontendId
    }
    previousQuestion {
      difficulty
      title
      titleSlug
      questionFrontendId
    }
  }
}
"""
var2 = {"currentQuestionSlug": "two-sum", "filters": {}, "envId": "", "envType": "problem-list"}


# Synchronous request
data = client.execute(query=query2, variables=var2)
print(data)  # => {'data': {'country': {'code': 'CA', 'name': 'Canada'}}}


# Asynchronous request
import asyncio

# data = asyncio.run(client.execute_async(query=query, variables=variables))
# print(data)  # => {'data': {'country': {'code': 'CA', 'name': 'Canada'}}}




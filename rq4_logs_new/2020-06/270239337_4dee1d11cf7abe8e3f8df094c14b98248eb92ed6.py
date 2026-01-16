import time
import GetOldTweets3 as got
import datetime

days_range = []

start = datetime.datetime.strptime("2020-03-01", "%Y-%m-%d")
end = datetime.datetime.strptime("2020-03-03", "%Y-%m-%d")
date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end-start).days)]

for date in date_generated:
    days_range.append(date.strftime("%Y-%m-%d"))

print("설정 트윗 수집 기간은 {}에서 {} 까지".format(days_range[0], days_range[-1]))
print("총 {}일간 데이터 수집".format(len(days_range)))

start_date = days_range[0]
end_date = (datetime.datetime.strptime(days_range[-1], "%Y-%m-%d")
            + datetime.timedelta(days=1)).strftime("%Y-%m-%d")

tweetCriteria = got.manager.TweetCriteria().setQuerySearch('코로나 OR 쿠팡') \
    .setSince(start_date) \
    .setUntil(end_date) \
    .setMaxTweets(-1)

start_time = time.time()
tweet = got.manager.TweetManager.getTweets(tweetCriteria)

print("Collecting data end... {0:0.2f} Minutes".format((time.time() - start_time) / 60))
print("total number of tweets is {}".format(len(tweet)))
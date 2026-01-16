# Import required libraries
from bs4 import BeautifulSoup
import requests

# Send a GET request to Hacker News
response = requests.get("https://news.ycombinator.com/news")
yc_webpage = response.text

# Parse the HTML content of the page
soup = BeautifulSoup(yc_webpage, "html.parser")

# Find all article title elements
articles = soup.find_all(class_="titleline")

# Initialize lists to store article texts and links
article_texts = []
article_links = []

# Extract text and link for each article
for article_tag in articles:
    # Get the article text
    text = article_tag.getText()
    article_texts.append(text)

    # Get the article link
    link = article_tag.select_one(selector="a").get("href")
    article_links.append(link)

# Extract upvote counts for all articles
# Convert the text scores to integers
article_upvote = [int(score.getText().split()[0]) for score in soup.find_all(name="span", class_="score")]

# Find the highest upvote count
max_vote = max(article_upvote)

# Get the index of the article with the highest upvote count
max_vote_index = article_upvote.index(max_vote)

# Print details of the article with the highest upvotes
print(article_texts[max_vote_index])  # Print the title of the most upvoted article
print(article_links[max_vote_index])  # Print the link of the most upvoted article
print(article_upvote[max_vote_index])  # Print the upvote count of the most upvoted article
import re
import requests
from enum import Enum
from bs4 import BeautifulSoup 

class VoteType(Enum):
    VOTE = 1
    RETRACTION = 2
    OTHER = 3

class Vote: 
    id = 0
    def __init__(self, name, vote_type, target, post_id):
        self.name = name
        self.type = vote_type
        self.target = target
        self.post_id = post_id

        self.id = Vote.id
        Vote.id += 1

    def __str__(self) -> str:
        return f"{self.name}: {self.type} on {self.target}"
    

class Player:
    def __init__(self, name):
        self.name = name
        self.votes = []
    
class Tally:
    votes = []
    player_list = {}

    def __init__(self, name) -> None:
        self.name = name
        self.count = 0
        self.voters = ""
        self.votersID = ""

    def update(self, vote):
        self.count += 1
        if self.voters != "":
            self.voters += ", "
            self.votersID += ", "
        self.voters += vote.name
        self.votersID += vote.name + f"[{vote.id}]"
    
    
    def add_vote(vote):
        if vote.name not in Tally.player_list.keys():
            player = Player(vote.name)
            Tally.player_list[vote.name] = player
        Tally.player_list[vote.name].votes.append(vote)
        Tally.votes.append(vote)




def get_posts(url, page=1, pages=None, start_post_id = None, end_post_id = None):
    results = None
  
    while pages == None or page <= pages:
        # Request the HTML document at the target URL, formatting it with the relevant page number
        r = requests.get("{}?page={}".format(url, page))

        # Parse the HTML so that soup commands can be used to look at particular tags
        soup = BeautifulSoup(r.content, 'html.parser')

        # if this is the first page:
        if not results: 
            # If this is the first page, set to results
            results = soup
        else:
            # Otherwise, add the new data to existing scraped data
            results.extend(soup)

        # Check if there's multiple pages the first time.
        if pages == None:
            block = soup.find("ul", "ipsPagination")
            if block: pages = int(block.attrs["data-pages"])
            else: pages = 1
        page += 1
    
    return results

# Given the style element containing the color attribute, returns the color hexcode
def find_color(col):
    str = col.get("style")
    if str.count(";") > 1: str = str.split(";")

    color = ""
    if type(str) is list:
        for s in str:
            if s.startswith("color:"):
                if s != "color:#000000":
                    color = s
    else:
        color = str

    if color == "":
        return None
    # Removes the leading "color:" and trailing ";" from the string
    if color.startswith("color:"):
        color = color[6:]
    if color.endswith(";"):
        color = color[:-1]

    # if started with <span style="color:rgb(231,76,60);>"
    if color.startswith("rgb"): # convert RGB to Hex

        rgb_col = color.split(";")[0] # Grab just the RGB(); portion
        rgb_col = rgb_col[4:-1].split(",") # Convert to a list of the 3 numbers.
        
        # Convert the list of RGB integers into a string in a hex color format
        # casts d to an int, and then formats the decimal integer 
        # as a hexidecimal value without the 0x in front 
        color = "#"
        for d in rgb_col:
            color += '{0:x}'.format(int(d)) 

    # Ensure any letters are lowercase
    color = color.lower()

    # otherwise string should already be in "#abcdef" form
    return color



def main():
    # The URL thread URL to consider
    url = "https://www.17thshard.com/forums/topic/175079-ag-10an-14-day-two-a-change-in-tune/"
    soup = get_posts(url)
    
    # Removes any quotes to avoid counting any votes found inside
    for b in soup("blockquote"):
        b.decompose()
    for s in soup.select('div[class*=ipsSpoiler]'):
        s.decompose()
    print("[b]Votes and Retractions[/b]")

    # Get a list of all post from page
    post = soup.select('article[id*=elComment]')

    for p in  post:
        # Get player name
        name = p.find("div",{"class":"cAuthorPane_content"}).find('a').text

        # Get the post ID for reference
        post_id = p.get("id")[10:]

        # Get player post
        content = p.find("div", {"class":"cPost_contentWrap"})

        # Find tags styled with color
        colors = content.select("[style*=color]")
        for c in colors:
            color = find_color(c)
            if color == None:
                continue
            vote_type = classify_color(color)
            vote_target = c.text.title().strip()
            if vote_target == "": continue

            if (vote_type != VoteType.OTHER):
                # process_vote(name, classify_color(color), c.text)
                if vote_type == VoteType.VOTE: vote_text = "votes on"
                else: vote_text = "retracts from"
                print(f"{name} {vote_text} {vote_target} (Post: {url}?do=findComment&comment={post_id})")
                vote = Vote(name, vote_type, vote_target, post_id)
                Tally.add_vote(vote)

    print("")
    final_votes_names = []
    final_votes = []
    for i in range(len(Tally.votes)-1,0,-1):
        vote = Tally.votes[i]
        if vote.name not in final_votes_names:
            if vote.type == VoteType.RETRACTION:
                if i > 0:
                    previous_vote = Tally.votes[i-1]
                    if (previous_vote.name == vote.name) and (previous_vote.type == VoteType.VOTE):
                        final_votes_names.append(vote.name)
                        final_votes.append(vote)
            elif vote.type == VoteType.VOTE:
                final_votes_names.append(vote.name)
                final_votes.append(vote)

    print("[b]Vote Tally[/b]")
    tally_counter = {}
    for vote in reversed(final_votes):
        if vote.target not in tally_counter:
            tally_counter[vote.target] = Tally(vote.target)
        t = tally_counter[vote.target]
        t.update(vote)

    sorted_dictionary = dict(sorted(tally_counter.items(), key=lambda item: item[1].count, reverse=True))

    for p in sorted_dictionary:
        player = sorted_dictionary[p]
        print(f"[b]{player.name}[/b] ({player.count}): {player.voters}")

    # print("")
    # print("[b]Vote Tally with vote order IDs[/b]")
    # for p in sorted_dictionary:
    #     player = sorted_dictionary[p]
    #     print(f"[b]{player.name}[/b] ({player.count}): {player.votersID}")



# given a color, returns an enum
def classify_color(col):
    red = [
        "#e74c3c", # Default color picker red
        "#c0392b", # Alternate color picker red (dark)
        "#d35400", # Alternate color picker red (browner)
        "#ff0000", # HTML Red
        "#8B0000", # HTML DarkRed
        "#dc143c", # HTML Crimson
        "#b22222", # HTML Firebrick
        "#800000"  # HTML Maroon
    ]
    green = [
        "#2ecc71",  # Default color picker green
        "#27ae60",  # Alt color picker green (dark)
        "#1abc9c",  # Alt color picker green (bluey)
        "#16a085",  # Alt color picker green (bluey, dark)
        "#008000",  # HTML Green
        "#006400",  # HTML DarkGreen
        "#228b22",  # HTML ForestGreen
        "#00ff00",  # HTML Lime
        "#32cd32",  # HTML LimeGreen
        "#2e8b57",  # HTML SeaGreen
        "#00ff7f"   # HTML SpringGreen

    ]
    if col in red:
        return VoteType.VOTE
    elif col in green:
        return VoteType.RETRACTION
    else:
        return VoteType.OTHER


# def process_vote(name, vote, target):
#     print(f"{name} {vote} {target}")

main()
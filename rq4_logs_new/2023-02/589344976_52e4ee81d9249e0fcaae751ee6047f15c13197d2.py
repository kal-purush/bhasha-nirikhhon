from typing import Dict, List
import os
import json
import os
from io import StringIO
from textwrap import dedent
from ruamel.yaml import YAML
import datetime
from pathlib import Path
from urllib.request import urlopen

######################################################
#                  Helper Functions                  #
######################################################
def write_content_to_file(formatted, save_dir):
    os.makedirs(save_dir, exist_ok=True)
    with open(os.path.join(save_dir, formatted["filename"]), "w") as f:
        f.write(formatted["content"])

def front_matters_from_dict(d):
    # Convert to dict
    d = json.loads(json.dumps(d))

    file_object = StringIO()
    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.dump(d, file_object)
    front_matter = file_object.getvalue()

    return front_matter

def get_filename(parsed):
    return parsed['year']+"-12-30-"+parsed['title'].replace(" ",'-') + ".md"

######################################################
#               publications Functions               #
######################################################    
def author_publications(author_id:str, api_key:str)->Dict[str, List[str]]:
  url = urlopen(f"https://serpapi.com/search.json?engine=google_scholar_author&author_id={author_id}&api_key={api_key}")
  data = json.loads(url.read())
  return data

def publication_info(info:str, author:str, save_dir=str)-> Dict[str,str]:
  parsed_info = {"title":info['title'], "venue":info['publication'],"names":info['authors'], "tags":'',
                 "link":info['link'],"author":author, "categories":"Publications" ,'year':info['year']}

  return parsed_info


def generate_publication_post(parsed:Dict[str,str]):
    """
    Format the parsed content into a string.
    """

    front_matter = front_matters_from_dict(parsed)
    top = dedent(f"---\n{front_matter}\n---\n")

    bottom = dedent(
        """
    *{{ page.names }}*
    **{{ page.venue }}**
    {% include display-publication-links.html pub=page %}
    ## Abstract
    
        """
    )

    try:
        content = top + bottom + str(parsed.get("abstract"))
    except TypeError as e:
        raise Exception(f"{e}\n{top=}\n{bottom=}\n{parsed=}") 

    return {
        "filename": get_filename(parsed),
        "content": content,
    }


######################################################
#                  Main(run pipeline)                #
######################################################
def main(save_dir="_posts/papers", site_data_dir="_data/", api_key:str=''):
    site_data_dir = Path(site_data_dir)

    yaml = YAML()
    yaml.preserve_quotes = True
    with open(site_data_dir / "authors.yml") as f:
        authors = yaml.load(f)
    
    for author in authors.values():
      author_id = author.get('google_scholar_id', False)
      name = author.get('name', False)
      publications = author_publications(author_id, api_key)

      if author_id == False:
        continue

      else:
        for pub in publications['articles']:
            parsed_info = publication_info(pub, name)
            pub_post = generate_publication_post(parsed_info)
            write_content_to_file(pub_post, save_dir)



if __name__ == "__main__":
  #setup SerpAPI account to get api_key
  main(save_dir="_posts/papers", site_data_dir="_data/",api_key="")
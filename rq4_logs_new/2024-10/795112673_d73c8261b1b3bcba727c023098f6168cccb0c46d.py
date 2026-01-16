import utilities


script_id = "blog_feed"



def get_blog_data():
  if utilities.use_test_data:
    blog_data = utilities.read_file("_data/blog_data_example.xml", file_type="xml")
    response = {'status': 200, 'attempts': 1, 'data': blog_data}
    utilities.log(response, context=f"{script_id}__get_blog_data")
    return response
  else:
    url = "https://paragraph.xyz/api/blogs/rss/@ethstaker"
    response = utilities.fetch(url, "GET", data_type="xml", context=f"{script_id}__get_blog_data")
    return response

def process_blog_data(blog_data):
  # example input
    # <?xml version="1.0" encoding="utf-8"?>
    # <rss xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:content="http://purl.org/rss/1.0/modules/content/" version="2.0"><script/>
    #   <channel>
    #     <title>ethstaker</title>
    #     <link>https://paragraph.xyz/@ethstaker</link>
    #     <description>Ethereum staking info & education </description>
    #     <lastBuildDate>Thu, 17 Oct 2024 13:28:01 GMT</lastBuildDate>
    #     <docs>https://validator.w3.org/feed/docs/rss2.html</docs>
    #     <generator>https://github.com/jpmonette/feed</generator>
    #     <language>en</language>
    #     <image>
    #       <title>ethstaker</title>
    #       <url>https://storage.googleapis.com/papyrus_images/155a1d5a6b9e5d4c8e20d2f758528e96</url>
    #       <link>https://paragraph.xyz/@ethstaker</link>
    #     </image>
    #     <copyright>All rights reserved</copyright>
    #     <item>
    #       <title>
    #         <![CDATA[ A Beginner’s Guide to Staking - Intro ]]>
    #       </title>
    #       <link>https://paragraph.xyz/@ethstaker/a-beginners-guide-to-staking-intro</link>
    #       <guid>JLLvghm7EA7pNwsJphka</guid>
    #       <pubDate>Thu, 27 Apr 2023 00:00:00 GMT</pubDate>
    #       <description>
    #       <![CDATA[ I’ve decided to write a beginner’s guide to Ethereum staking. It’s meant to target people who are kind of interested in crypto, but not quite sure wha... ]]>
    #       </description>
    #       <content:encoded>
    #       <![CDATA[ <p>I’ve decided to write a beginner’s guide to Ethereum staking. It’s meant to target people who are <em>kind of</em> interested in crypto, but not quite sure what Ethereum really is or why you’d want to stake. It’ll be as accessible as I can manage and come out in six parts twice a week:</p><ul><li><p>What is Ethereum?</p></li><li><p>What is a validator?</p></li><li><p>How staking stays healthy long-term (and how your choices help)</p></li><li><p>Broad overview of some issues that Ethereum staking faces today</p></li><li><p>Factors that should play into your decision to stake or not</p></li><li><p>Finding your best way to stake</p></li></ul><p>Each section should take 15 minutes or less to read and will be accompanied by some links to learn more. This is meant to make you feel culturally oriented in the staking community before you dive into a technical guide.</p><p>If you find any technical inaccuracies in these posts, please DM me on twitter and let me know before I get roasted by the entire internet: <a target="_blank" rel="noopener noreferrer nofollow ugc" class="dont-break-out dont-break-out" href="http://twitter.com/nixorokish">twitter.com/nixorokish</a></p> ]]>
    #       </content:encoded>
    #       <author>ethstaker@newsletter.paragraph.xyz (nixo)</author>
    #       <category>web3</category>
    #       <category>cryptocurrency</category>
    #       <category>staking</category>
    #       <category>blockchain</category>
    #       <category>ethereum</category>
    #       <category>beginner</category>
    #     </item>
    #   </channel>
    # </rss>
  blog_data_xml = blog_data.replace('& ','&amp; ')
  blog_data_json = utilities.xml2json(blog_data_xml)
  utilities.log(blog_data_json, context=f"{script_id}__process_blog_data")
  return [blog_data_xml, blog_data_json]

def save_blog_data(blog_data_xml, blog_data_json):
  utilities.save_to_file(f"_data/blog_data.json", blog_data_json, context=f"{script_id}__save_blog_data")
  utilities.save_to_file(f"_data/blog_data.xml", blog_data_xml, context=f"{script_id}__save_blog_data", data_type="xml")



def update_blog_feed():
  blog_data = get_blog_data()
  if (blog_data["status"] == 200):
    blog_data_formatted = process_blog_data(blog_data["data"])
    save_blog_data(blog_data_formatted[0], blog_data_formatted[1])
  else: 
    error = f"Bad response"
    utilities.log(f"{error}: {script_id}")
    utilities.report_error(error, context=f"{script_id}__update_blog_feed")
    return




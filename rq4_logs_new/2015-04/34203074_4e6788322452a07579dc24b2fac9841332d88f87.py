import urllib.request, praw, csv, time, sys, datetime, string, progressbar

from bs4 import BeautifulSoup

title_s = "title:"
description_s = "description:"
type_s = "type:"
map_s = "map:"
preview_s = "preview:"
keywords = (title_s, description_s, type_s, map_s, preview_s)


def get_comments(thread_id):
    r = praw.Reddit("Maptest Committee Extractor")
    thread_data = r.get_submission(submission_id = thread_id)
    thread_data.replace_more_comments(limit = None, threshold = 0)
    return thread_data.comments

def fix_list(in_list):
    if "\n* " not in in_list:
        return in_list
    out_list = in_list
    while "*  " in out_list:
        out_list = out_list.replace("*  ", "* ")
    init_pos = out_list.find("* ")
    if init_pos == -1:
        return in_list
    for i in keywords:
        endl_pos = out_list.find("\n")
        position = out_list.lower().find("* " + i)
        if position == -1:
            stars = ["*", "**"]
            for j in stars:
                position = out_list.lower().find("* " + j + i)
                if position != -1:
                    break
        endl_fix = ""
        if position > endl_pos:
            endl_fix = "\n"
        if position != -1:
            out_list = out_list[:position] + endl_fix + out_list[position + 2:]
    return out_list


#    for i in keywords:
#        out_list = out_list.replace("* " + i, "\n" + i)
#    while out_list[0][0] == "\n":
#        out_list[0] = out_list[0][1:]
#    out_list = out_list[init_pos + 2:]
#    out_list = out_list.replace("\n* ", "\n\n")
    return out_list

def get_string(comment_body, element):
    while "\n " in comment_body:
        comment_body = comment_body.replace("\n ", "\n")
    while "   \n" in comment_body:
        comment_body = comment_body.replace("   \n", "  \n")
    while "\"" in comment_body:
        comment_body = comment_body.replace("\"", "\'")
    comment_body = comment_body.replace("  \n", "\n\n")
    while "\n\n\n" in comment_body:
        comment_body = comment_body.replace("\n\n\n", "\n\n")
    while " :" in comment_body:
        comment_body = comment_body.replace(" :", ":")
    comment_body = comment_body.replace("___", "")
    comment_body = fix_list(comment_body)
    comment_list = comment_body.split("\n\n")
    for i in range(len(comment_list)):
        comment_list[i] = comment_list[i].replace("\n", "")
        strip_comment = comment_list[i].replace("*", "")
        l_comment = strip_comment.lower()
        current_pos = l_comment.find(element)
        if current_pos != -1:
            modi = 0
            http_pos = comment_list[i].find("http")
            end_website = comment_list[i][http_pos:].find(" ")
            if http_pos != -1:
                beg_com = comment_list[i][:http_pos].replace("*", "")
                end_com = comment_list[i][end_website:].replace("*", "")
                comment_list[i] = beg_com + \
                                  comment_list[i][http_pos:end_website] + \
                                  end_com
                while comment_list[i][-1] == "*":
                    comment_list[i] = comment_list[i][:-1]
            else:
                comment_list[i] = comment_list[i].replace("*", "")
            while ": " in comment_list[i]:
                comment_list[i] = comment_list[i].replace(": ", ":")
            current_pos += len(element)
            return comment_list[i][current_pos:]
    return False


def get_comment_data(comment):
    comment_data = {}
    comment_info = [title_s, description_s, type_s, map_s, preview_s]
    for i in comment_info:
        new_data = get_string(comment.body, i)
        comment_data[i[:-1]] = new_data
    comment_data["timestamp"] = comment.created_utc
    comment_data["link"] = comment.permalink
    return comment_data

def fix_bad_info(maps):
    bad_titles = 0
    map_info = [description_s, type_s, map_s, preview_s]
    base_s = "Missing "
    for i in maps:
        if not i["title"]:
            bad_titles += 1
            new_title = "Missing title " + str(bad_titles)
            i["title"] = new_title
        if i["preview"]:
            if "(" in i["preview"] and ")" in i["preview"]:
                if "[" in i["preview"] and "]" in i["preview"]:
                    start_pos = i["preview"].find("(")
                    end_pos = i["preview"].find(")")
                    i["preview"] = i["preview"][start_pos + 1:end_pos]
            if " " in i["preview"]:
                split_p = i["preview"].find(" ")
                i["preview"] = i["preview"][:split_p]
        if i["map"]:
            i["map"] = i["map"].replace("#", "")
            if "(" in i["map"] and ")" in i["map"]:
                if "[" in i["map"] and "]" in i["map"]:
                    start_pos = i["map"].find("(")
                    end_pos = i["map"].find(")")
                    i["map"] = i["map"][start_pos + 1:end_pos]
            if "jukejuice" not in i["map"]:
                i["map"] = "Improper Test Link"
    return maps


def sort_maps(maps):
    maps = [i for i in maps]
    new_maps = []
    sorted_maps = []
    while len(maps) > 0:
        current_lowest = maps[0]
        lowest = maps[0]["timestamp"]*1000000000
        current_map = None
        for element in maps:
            if element["link"] not in sorted_maps:
                if element["timestamp"] <= lowest:
                    current_lowest = element
                    lowest = element["timestamp"]
        sorted_maps.append(current_lowest["title"])
        new_maps.append(current_lowest)
        maps.remove(current_lowest)
    return new_maps

def get_map_id(url):
    if url:
        url = url.replace("cvps", "maps")
        elements = url.split("/")
    try:
        mapid = int(elements[-1])
        return str(mapid)
    except:
        try:
            data = urllib.request.urlopen(url)
            page = BeautifulSoup(data)
            page_str = str(page)
            str_beg = page_str.find("/maptest/")
            if str_beg > 0:
                length = page_str[str_beg:].find("\" role")
                return page_str[str_beg  + 9: str_beg + length]
        except:
            return None



def fix_image(image):
    if "imgur" in image and not "i.imgur" in image:
        image = image[:7] + "i." + image[7:] + ".png"
        image = image.replace(" ", "")
    return image


def write_csv(maps):
    out_strings = []
    n_maps = 0
    test_str1 = "=HYPERLINK(\"http://maps.jukejuice.com/maptest/"
    alt_test1 = "=HYPERLINK(\"http://unfortunate-maps.jukejuice.com/maptest/"
    test_str2 = "/ca\", \"Test Map\")"
    image_str1 = "=image(\""
    image_str2 = "\", 1)"
    preview_str1 = "=HYPERLINK(\""
    preview_str2 = "\", \"Preview\")"
    link_str1 = "=HYPERLINK(\""
    link_str2 = "\", \""
    link_str3 = "\")"
    i = 0
    progress = progressbar.Progress(len(maps))
    for cur_map in maps:
        n_maps += 1
        map_id = get_map_id(cur_map["map"])
        test_map = "Improper Test Format"
        if map_id:
            if "maps.jukejuice" in cur_map["map"]:
                test_map = test_str1 + map_id + test_str2
            if "unfortunate" in cur_map["map"]:
                test_map = alt_test1 + map_id + test_str2
        else:
            if cur_map["map"]:
                test_map = "=HYPERLINK(\"" + cur_map["map"] + "\", \"Test Map\")"
        map_image = "Improper Image Link"
        map_preview = "improper Image Link"
        if cur_map["preview"]:
            cur_map["preview"] = fix_image(cur_map["preview"])
            map_image = image_str1 + cur_map["preview"] + image_str2
            map_preview = preview_str1 + cur_map["preview"] + preview_str2
        else:
            if"HYPERLINK" in test_map:
                new_preview = cur_map["map"][:cur_map["map"].find(".com/") + 5]
                new_id = cur_map["map"].split("/")[-1]
                new_preview += "static/previews/" + new_id + ".png"
                map_image = image_str1 + new_preview + image_str2
                map_preview = preview_str1 + new_preview + preview_str2
        map_desc = "No Description"
        if cur_map["description"]:
            map_desc = cur_map["description"]
        map_link = link_str1 + cur_map["link"] + link_str2
        map_link += cur_map["title"] + link_str3
        map_type = "No Type Given"
        if cur_map["type"]:
            map_type = cur_map["type"]
        out = [map_link, map_type, test_map, map_image, map_preview, map_desc]
        out_strings.append(out)
        progress.Increment()
    progress.End()
    filename = "maps_{}.txt".format(datetime.date.today().strftime("%B_%d_%Y")) 
    with open(filename, "w") as f:
        a = csv.writer(f, delimiter='\t', lineterminator='\n')
        a.writerows(out_strings)
    return filename, n_maps

def summary(filename, n_maps, maps):
    print("Filename:", filename, "maps:", n_maps, "Posts:", len(maps))


def main():
    thread_id = sys.argv[1]
    sys.stdout.write("Getting comments... ")
    comments = get_comments(thread_id)
    sys.stdout.write("Done\nParsing comment data...")
    maps = []
    for comment in comments:
        comment_data = get_comment_data(comment)
        maps.append(comment_data)
    sys.stdout.write("Done\nFixing Bad Info...")
    maps = fix_bad_info(maps)
    sys.stdout.write("Done\nSorting Maps...")
    maps_sorted = sort_maps(maps)
    sys.stdout.write("Done\nWriting csv...\n")
    filename, n_maps = write_csv(maps_sorted)
    sys.stdout.write("Done\n")
    summary(filename, n_maps, maps)


if __name__ == "__main__":
    main()
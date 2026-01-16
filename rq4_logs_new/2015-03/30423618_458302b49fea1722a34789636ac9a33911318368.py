__author__ = 'Josue Carbonel'


def key_changer(dict_name_to_change, erase_letters_on_key, old_key_list=None, new_key_list=None):

    if old_key_list is None:
        old_key_list = dict_name_to_change.keys()

    for index in range(0, len(old_key_list)):
        try:
            new_key = old_key_list[index].replace(erase_letters_on_key, '')
            dict_name_to_change[new_key] = dict_name_to_change[old_key_list[index]]
            del dict_name_to_change[old_key_list[index]]
        except:
            pass

def get_image_000x(school, img_txt_file):
    social_media_file = open(img_txt_file).read()
    temp_school = school.replace(' ', '')
    social_media_file = social_media_file.split('\n')
    social_media_file = filter(None, social_media_file)

    index = 0
    img_125x = ''
    school_found = False
    while index < len(social_media_file):
        list_school = social_media_file[index].replace(' ', '')
        if temp_school == list_school:
            img_125x = social_media_file[index + 1]
            school_found = True
            break
        else:
            index += 2
        if school_found is True:
            break
    return img_125x

def get_facebook_and_twitter(school):
    social_media_file = open('facebook_twitter_file.txt').read()
    temp_school = school.replace(' ', '')
    social_media_file = social_media_file.split('\n')
    social_media_file = filter(None, social_media_file)

    index = 0
    twitter = ''
    facebook = ''
    school_found = False
    while index < len(social_media_file):
        list_school = social_media_file[index].replace(' ', '')
        if temp_school == list_school:
            twitter = social_media_file[index + 1]
            facebook = social_media_file[index + 2]
            school_found = True
            #print(twitter, facebook)
            break
        else:
            index += 3
        if school_found is True:
            break
    return twitter, facebook
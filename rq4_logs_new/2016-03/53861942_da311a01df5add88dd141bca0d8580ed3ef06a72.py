#-------------------------------------------------------------------------------
# Name:        модуль1
# Purpose:
#
# Author:      Дмитрий
#
# Created:     12.03.2016
# Copyright:   (c) Дмитрий 2016
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import XMLmapper
def main():
    mapper.add_parse_objects('event')
    #
    #mapper.add_parse_tags('title','value')
    mapper.add_parse_tags('runtime','value')
    mapper.add_parse_tags('age_restricted','value')
    mapper.add_parse_tags('tags','list')
    mapper.add_parse_tags('gallery','list')
    mapper.add_parse_tags('text','value')
    mapper.add_parse_tags('tag','value')
    mapper.add_parse_tags('description','value')
    mapper.add_parse_tags('stage_theatre','value')
    mapper.find('event')
    #print(mapper.results_of_element[1])
    #mapper.tojson()
    #print(mapper.is_parse_tag('title'))
    mapper.tag_filter()

if __name__ == '__main__':
    main()
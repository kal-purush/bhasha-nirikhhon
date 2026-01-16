#!/usr/bin/env python
# -*- coding: utf-8 -*-
# from __future__ import unicode_literals
import requests
from lxml.html import parse
import MySQLdb
import sys
import unicodedata
import string
import itertools
# from bs4 import UnicodeDammit

cnxn = MySQLdb.connect(host="localhost", user="root", passwd="harshit@123", db="college_info", charset="utf8", use_unicode=True)
cursor = cnxn.cursor()
cnxn.autocommit(True)

query001 = "set names 'utf8mb4'"
query002 = "SET CHARACTER SET utf8mb4"

# making database accept utf8mb4 as the data format in their columns
cursor.execute(query001)
cursor.execute(query002)

# query007 = "Delimiter $$ " \
#            "drop procedure if exists add_unique_columns $$" \
#            "create procedure add_unique_columns(in col_name varchar(50), in tab_name varchar(50), out status varchar(10))" \
#            "begin" \
#            "if not exists ((select id from information_schema.columns where table_schema='college_info' and table_name=tab_name and column_name = col_name)) then" \
#            "alter table tab_name add col_name varchar(400) null;" \
#            "set status = 'True';" \
#            "else" \
#            "set status = 'False';" \
#            "end if;" \
#            "end $$" \
#            "delimiter ;"
#
# cursor.execute(query007)


try:
    headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:40.0) Gecko/20100101 Firefox/40.0'}
    # url = requests.get(url='https://colleges.niche.com/rankings/top-public-universities', headers=headers)
    # url1 = requests.get(url='https://colleges.niche.com/rankings/top-private-universities', headers=headers)
    url2 = requests.get(url='http://colleges.usnews.rankingsandreviews.com/best-colleges/rankings/national-universities/data', headers=headers)

    # file_pub = open('public_univ_niche.html', 'w')
    # file_pvt = open('private_univ_niche.html', 'w')
    file_us_news = open('college_data_us_news.html', 'w')

    # file_pub.write(url.text.encode('ascii', 'ignore'))
    # file_pvt.write(url1.text.encode('ascii', 'ignore'))
    file_us_news.write(url2.text.encode('utf-8', 'ignore'))

    # doc = parse('public_univ_niche.html').getroot()
    # doc.make_links_absolute('https://colleges.niche.com', resolve_base_href=True)
    #
    # doc7 = parse('private_univ_niche.html').getroot()
    # doc7.make_links_absolute('https://colleges.niche.com', resolve_base_href=True)
    #
    # college_link = []
    # college_name = []
    #
    # for data in doc.cssselect('div.name a'):
    #     college_link.append(data.get('href'))
    #     college_name.append(data.text_content())
    #
    # for data7 in doc7.cssselect('div.name a'):
    #     college_link.append(data7.get('href'))
    #     college_name.append(data7.text_content())
    #
    # best_about_college = []
    # worst_about_college = []
    # header = []
    # data_about_header = []
    #
    # # check =0
    # for i, val in enumerate(college_link):
    #
    #     # if check < 3:
    #     file_college = open('public_univ_niche_colleges_data.html', 'w')
    #     url_col = requests.get(url=val, headers=headers)
    #     file_college.write(url_col.text.encode('utf-8', 'ignore'))
    #
    #     doc1 = parse('public_univ_niche_colleges_data.html').getroot()
    #     doc1.make_links_absolute('https://colleges.niche.com', resolve_base_href=True)
    #
    #
    #     for link in doc1.cssselect('li.noChildren a'):
    #
    #         if link.text_content() == 'The Best & Worst':
    #             file_ranking = open('public_univ_niche_details.html', 'w')
    #             url_det = requests.get(url=link.get('href'), headers=headers)
    #             file_ranking.write(url_det.text.encode('utf-8', 'ignore'))
    #
    #             doc2 = parse('public_univ_niche_details.html').getroot()
    #             doc2.make_links_absolute('https://colleges.niche.com', resolve_base_href=True)
    #
    #             sub_best_about_college = []
    #             sub_worst_about_college = []
    #
    #             count = 0
    #             for data in doc2.cssselect('div.bestWorstList'):
    #                 if count == 0:
    #                     for info in data.cssselect('div.text'):
    #                         # text = unicodedata.normalize('NFKD', unicode(info.text_content())).encode('utf8', 'ignore')
    #                         sub_best_about_college.append(info.text_content())
    #                 elif count ==1:
    #                     for info in data.cssselect('div.text'):
    #                         # text = unicodedata.normalize('NFKD', unicode(info.text_content())).encode('utf8', 'ignore')
    #                         sub_worst_about_college.append(info.text_content())
    #                 else:
    #                     break
    #                 count +=1
    #
    #             sub_header = []
    #             sub_data_about_header = []
    #
    #             best_about_college.append(sub_best_about_college)
    #             worst_about_college.append(sub_worst_about_college)
    #             # print best_about_college, " : ", worst_about_college
    #
    #             for data in doc2.cssselect('div.ss'):
    #                 for item in data.cssselect('div'):
    #                     if item.get('class') == "fact fullwidth":
    #                         # print "inside"
    #                         for reduntant in item.cssselect('div.title'):
    #                             # val = item.xpath("./div [@class='title']")
    #                             # print val
    #                             # print reduntant
    #                             # text = unicodedata.normalize('NFKD', unicode(reduntant.text_content())).encode('utf8', 'ignore')
    #                             # text1 = unicodedata.normalize('NFKD', unicode(reduntant.tail.strip(' \n'))).encode('utf8', 'ignore')
    #                             sub_header.append(reduntant.text_content())
    #                             sub_data_about_header.append(reduntant.tail.strip(' \n\r'))
    #
    #                 # break
    #
    #             header.append(sub_header)
    #             data_about_header.append(sub_data_about_header)
    #             # print header, " : ", data_about_header
    #
    #             break
    #
    #         else:
    #             continue
    #         #
    #         # check +=1
    #     #
    #     # else:
    #     #     break
    #
    # try:
    #
    #     for key,data in enumerate(data_about_header):
    #         col_current = []
    #         query007 = """select column_name from information_schema.columns where table_schema='college_info' and table_name='college_details_niche_parts'"""
    #         cursor.execute(query007)
    #         for row in cursor:
    #             col_current.append(row[0])
    #
    #         # print col_current
    #
    #         query = """insert into college_details_niche(college_name, best_things, worst_things) values("""
    #         query00 = """insert into college_details_niche_parts(college_name"""
    #         query1 = """alter table college_details_niche_parts add column("""
    #         count = 0
    #         # result_query = ""
    #         for item in header[key]:
    #             # print item
    #             act_val = item.replace("\'","").replace(' ','_').replace('-','_').replace('?','').strip()
    #             if act_val not in col_current:
    #                 print act_val
    #                 if count == 0:
    #                     query1 += act_val+""" varchar(300) NULL"""
    #                     # query00 += str(act_val)
    #                 else:
    #                     query1 += """, """+act_val+""" varchar(300) NULL"""
    #
    #             # cursor.execute("call add_unique_column("+str(item.replace("\'","").replace(' ','_').replace('-','_').strip())   +",'college_details_niche',@status)")
    #             # cursor.execute('select @status')
    #             # for val in cursor:
    #             #     if val == 'True':
    #                 query00 += """, """+str(act_val)
    #
    #                 count +=1
    #
    #             else:
    #                 query00 += """, """+str(act_val)
    #
    #
    #             # break
    #
    #         query00 += """) values("""
    #         query1 += """)"""
    #         if count > 0:
    #             # print query1+"\n\n"
    #             cursor.execute(query1)
    #             print "\n\n"
    #         # print "success"
    #         # best_things = ""
    #         # worst_things = ""
    #         #
    #         # for item,item1 in zip(best_about_college[key],worst_about_college[key]):
    #         best_things = '--'.join(best_about_college[key])
    #         worst_things = '--'.join(worst_about_college[key])
    #
    #         query += "'"+str(college_name[key].replace(',','').strip().encode('ascii','ignore'))+"', '"+str(best_things.replace(',','').replace('(','').replace(')','').replace("\'","").strip().encode('ascii','ignore'))+"', '"+str(worst_things.replace(',','').replace('(','').replace(')','').replace("\'","").strip().encode('ascii','ignore'))+"')"
    #         # print query
    #         # print data
    #         # test_query = "'mark','mark','mark'"
    #         query00 += "'"+str(college_name[key].replace(',','').strip().encode('ascii','ignore'))+"'"
    #         for item in data:
    #             # print item
    #             # query += """,%s"""
    #             # test_query += ",'MArk'"
    #
    #             query00 += ", '"+str(item.replace(',','').replace('(','').replace(')','').replace("\'","").strip().encode('ascii','ignore'))+"'"
    #             # print query+"\n\n"
    #         # print test_query
    #         query00 += """)"""
    #         # print result_query
    #         # print query+"\n\n"
    #         # check_len = result_query.split(',')
    #         # print len(check_len)
    #         cursor.execute(query)
    #         cursor.execute(query00)
    #
    # except Exception,ex:
    #     print "Error in adding new columns to the table ", ex
    #     sys.exit(0)

    doc = parse('college_data_us_news.html').getroot()
    doc.make_links_absolute('http://colleges.usnews.rankingsandreviews.com', resolve_base_href=True)
    # college_link = []
    college_details = []
    other_college_details =[]
    college_name = []
    check = True
    c=0
    while check:
        c+=1
        print "\n", c, "started"
        try:

            for data in doc.cssselect('table tbody tr'):
                # print "inside"
                count = 1
                sub_college_details = []
                save,save1,save2,save3 = [],[],[],[]

                for item in data.cssselect('td'):
                    # print "new td"
                    if 3 <= count <= 7:
                        # print "inside other"
                        # check_val =  item.text_content().strip('\n ').startswith('in-state:')
                        # print check_val
                        if item.text_content().strip(' \n\r').startswith('in-state:'):
                            col_arr = item.text_content().strip(' \n\r').split(',')
                            data_check = ','.join(col_arr[2:])
                            # print data_check.strip(' '+string.punctuation+string.letters)
                            sub_college_details.append(data_check.strip(' \n\r'+string.punctuation+string.letters))
                            # continue

                        else:
                            sub_college_details.append(item.text.strip(' \n\r'+string.punctuation))

                    elif count == 2:
                        # print "inside 2"
                        for link in item.cssselect('a'):
                            col_name = u''+link.text_content()+''.replace('—​','-')
                            inside_college_link = link.get('href')
                            check_3 = 0
                            male_female_ratio = ""

                            url22 = requests.get(url=inside_college_link, headers=headers)
                            file_us_news1 = open('college_data_us_news.html', 'w')
                            file_us_news1.write(url22.text.encode('utf-8', 'ignore'))
                            doc3 = parse('college_data_us_news.html').getroot()
                            doc3.make_links_absolute('http://colleges.usnews.rankingsandreviews.com', resolve_base_href=True)

                            for item1 in doc3.xpath("//div [@class = 't-slack']/div"):
                                # print "inside this"
                                # print item1.xpath("//span [@class = 't-small t-subdued']/text()")
                                # print item1.xpath("//span [@class = 't-small t-subdued']/preceding-sibling::span/text()")
                                # print item1.xpath("//span/span [@class = 't-small t-subdued']/preceding-sibling::span/text()")
                                ck = 0
                                for inner in item1.xpath("//span/span [@class = 't-small t-subdued']/preceding-sibling::span/text()"):
                                    # print val11
                                    if ck == 0:
                                        # print int(inner.strip(' \n\r'+string.punctuation).replace(',',''))
                                        # print inner
                                        # if inner.strip(' \n\r').endswith('%'):
                                            # print inner
                                        # if int(inner.strip(' \n\r'+string.punctuation).replace(',','')) < 100 :
                                        male_female_ratio += inner.strip(' \n\r'+string.punctuation) + "-"
                                        # else:
                                        #     break
                                    elif ck == 1:
                                        # print inner
                                        # if inner.strip(' \n\r').endswith('%'):
                                        male_female_ratio += inner.strip(' \n\r'+string.punctuation)
                                        break
                                    ck+=1
                                    # break
                                    # cc = 0
                                    # if inner.strip(' \n\r') == 'male':
                                    #     # print "more insode"
                                    #     print item1.xpath("//span [@class = 't-small t-subdued']/preceding-sibling::span/text()")
                                    #     male_female_ratio += inner.xpath("/preceding-sibling::span").strip(' \n\r')+"-"
                                    # elif inner.strip(' \n\r') == 'female':
                                    #     for sib in inner.itersiblings(preceding = True):
                                    #         male_female_ratio += sib.text_content().strip(' \n\r')
                                break
                            # print "m-f", male_female_ratio
                            # print male_female_ratio
                            save2.append(male_female_ratio)

                            for item0,item1,item2,item3 in itertools.izip_longest(doc3.xpath("//div [@id='directoryPageSection-general_information']"),doc3.xpath("//div [@id='directoryPageSection-Academic_life']"),doc3.xpath("//div [@id='directoryPageSection-Student_life']"),doc3.xpath("//div [@id='directoryPageSection-Paying_for_school']")):
                                for val,val1,val2,val3 in itertools.izip_longest(item0.cssselect('table tbody tr'),item1.cssselect('table tbody tr'),item2.cssselect('table tbody tr'),item3.cssselect('p')):
                                  # for data,data1,data2,data3 in zip(val.cssselect('td'),val1.cssselect('td'),val2.cssselect('td'),val3.cssselect('td')):
                                  #   print val, val1, val2, val3
                                  #   print val.cssselect("td.column-first").text_content().strip(' \n\r')
                                    # sys.exit(0)
                                    # print len(val.xpath("//td [@class = 'column-first']/text()"))
                                    # print len(val.xpath("//td [@class = 'column-last']/span/text()"))
                                    if val is not None:
                                        val_saved = []
                                        for inner in val:
                                            # for check_val in val.iter(tag='td'):
                                            # print inner.text_content().strip(' \n\r'+string.punctuation)
                                            # break
                                            val_saved.append(inner.text_content().strip(' \n\r'+string.punctuation))
                                                # print inner.next().cssselect('span').text_content().strip(' \n\r'+string.punctuation)
                                        if val_saved[0] in ('School type','Religious affiliation', 'Setting', '2014 Endowment'):
                                                # data = inner.xpath('td /following::td/text()')
                                                # print data
                                                save.append(val_saved[1])
                                                # break
                                                # print i
                                                # print data.xpath("//following::td/span/text()")[i+3]
                                                # save.append(main.xpath("//following::td/span/text()")[0].strip(' \n\r'+string.punctuation))
                                    # print save
                                    # sys.exit(0)
                                        # print "check0"
                                        # print val.xpath("//td [@class = 'column-last']/span/text()").strip(' \n\r'+string.punctuation)

                                    # if val1 is not None and val1.xpath("/td [@class = 'column-first']/text()").strip(' \n\r') in ('Student-faculty ratio','4-year graduation rate'):
                                    #     save1.append(val1.xpath("/td [@class = 'column-last']/span/text()").strip(' \n\r'+string.punctuation))
                                    #     print "check1"
                                    #     print val1.xpath("/td [@class = 'column-last']/span/text()").strip(' \n\r'+string.punctuation)
                                    # elif val1 is not None and val1.xpath("/td [@colspan = '2']/text()").strip(' \n\r') == '':
                                    #     sub_save1 = ""
                                    #     print "check1_1"
                                    #     for data1 in val1.cssselect("td table tbody tr"):
                                    #         sub_save1 += data1.xpath("/td [@class = 'column-first column-odd']/text()").strip(' \n\r'+string.punctuation)+", "
                                    #
                                    #   # if not sub_save1:
                                    #     save1.append(sub_save1)

                                    try:

                                        if val1 is not None:
                                            val_inner = ""
                                            val_saved = []
                                            for inner in val1:
                                                # print inner
                                                # print inner.getparent()
                                                # print inner.get('class')
                                                if inner is not None and inner.getparent().get('class') == 'extra-row top_majors-extra-row':
                                                    # print inner.iter(tag='table')
                                                    if inner.iter(tag='table') is not None:
                                                        for inner_item in inner.cssselect('table tbody tr'):
                                                            # print inner_item
                                                            for more_inner in inner_item.cssselect('td'):
                                                                val_inner += more_inner.text_content().strip(' \n\r'+string.punctuation)+"--"
                                                                break
                                                        if val_inner == "":
                                                            continue
                                                        else:
                                                            save1.append(val_inner)
                                                            # print save1
                                                            break

                                                else:
                                                # for check_val in val.iter(tag='td'):
                                                # print inner.text_content().strip(' \n\r'+string.punctuation)
                                                # break
                                                    val_saved.append(inner.text_content().strip(' \n\r'+string.punctuation))
                                                    # print inner.next().cssselect('span').text_content().strip(' \n\r'+string.punctuation)
                                            # print val_saved
                                            # print (not not val_saved)
                                            if not not val_saved and val_saved[0] in ('Student-faculty ratio','4-year graduation rate'):
                                                # print val_saved[0]
                                                # data = inner.xpath('td /following::td/text()')
                                                # print data
                                                save1.append(val_saved[1][:4].strip(' \n\r'+string.punctuation))
                                                # sys.exit(0)
                                                # break
                                                # print i
                                                # print data.xpath("//following::td/span/text()")[i+3]
                                                # save.append(main.xpath("//following::td/span/text()")[0].strip(' \n\r'+string.punctuation))


                                        # print save1
                                    except Exception,ee:
                                        print "Error in save1", ee

                                    # if val2 is not None and val2.xpath("/td [@class = 'column-first']/text()").strip(' \n\r') == 'Collegiate athletic association':
                                    #     print "check2"
                                    #     save2.append(val2.xpath("/td [@class = 'column-last']/span/text()").strip(' \n\r'+string.punctuation))

                                    if val2 is not None:
                                        val_saved = []
                                        for inner in val2:
                                            # for check_val in val.iter(tag='td'):
                                            # print inner.text_content().strip(' \n\r'+string.punctuation)
                                            # break
                                            val_saved.append(inner.text_content().strip(' \n\r'+string.punctuation))
                                                # print inner.next().cssselect('span').text_content().strip(' \n\r'+string.punctuation)
                                        if val_saved[0] == 'Collegiate athletic association':
                                                # data = inner.xpath('td /following::td/text()')
                                                # print data
                                                save2.append(val_saved[1].strip(' \n\r'+string.punctuation))
                                                # break
                                                # print i
                                                # print data.xpath("//following::td/span/text()")[i+3]
                                                # save.append(main.xpath("//following::td/span/text()")[0].strip(' \n\r'+string.punctuation))
                                    # print save2

                                    check_3 +=1
                                    if check_3 == 1:
                                        val3_arr = (val3.text_content().strip(' \n\r'+string.punctuation+string.letters).split(' '))
                                        save3.append(val3_arr[0])

                            college_name.append(unicodedata.normalize('NFKD', col_name).encode('utf8', 'ignore'))
                            break
                    count+=1
                pre_college_details = []
                # pre_college_details.extend(sub_college_details)
                # print col_name
                # print save
                # print save1
                # print save2
                # print save3
                # print "\n\n"

                pre_college_details.extend(save)
                pre_college_details.extend(save1)
                pre_college_details.extend(save2)
                pre_college_details.extend(save3)
                college_details.append(sub_college_details)
                other_college_details.append(pre_college_details)

            for item in doc.xpath("//p [@id='pagination']"):
                flag=0
                for link in item.cssselect('a'):
                    # print "get in to it"
                    # print unicodedata.normalize('NFKD', u''+item.text_content()+'').encode('utf8', 'ignore')
                    compare_data =  u''+link.text_content()+''
                    if unicodedata.normalize('NFKD', compare_data).encode('utf8', 'ignore') == unicodedata.normalize('NFKD', u'Next »').encode('utf8', 'ignore'):
                        # print "jabbahj"
                        flag=1
                        link1 = link.get('href')
                        # print link1

                        url2 = requests.get(url=link1, headers=headers)
                        file_us_news = open('college_data_us_news.html', 'w')
                        file_us_news.write(url2.text.encode('utf-8', 'ignore'))
                        doc = parse('college_data_us_news.html').getroot()
                        doc.make_links_absolute('http://colleges.usnews.rankingsandreviews.com', resolve_base_href=True)
                        break
                    else:
                        continue

                if flag==0:
                    check = False
                break

        except Exception, exx:
            print "Error in adding values : ", exx

    # print college_name, " :: ", college_details, " : ", other_college_details

    try:
        # print college_details
        # print "\n\n"
        # print other_college_details
        # print "\n\n"
        # print college_name
        # sys.exit(0)
        k = 0
        for val,val1 in zip(college_details,other_college_details):
            # print val
            # print val
            # print "/n/n"
            # print val1
            query = """insert into college_details_us_news(college_name,tuition_cost,total_population,acceptance_rate,retention_rate,6_year_graduation_rate,school_type,religious_affiliation,location_setting,endowment,faculty_student_ratio,4_year_graduation_rate,popular_majors,male_female_ratio,athletic_association,financial_aid_pct) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            cursor.execute(query, (college_name[k], str(val[0]), str(val[1]), str(val[2]), str(val[3]), str(val[4]), str(val1[0]), str(val1[1]), str(val1[2]), str(val1[3]), str(val1[4]), str(val1[5]), str(val1[6]), str(val1[7]), str(val1[8]), str(val1[9])))
            k+=1
    except Exception, ex:
        print "Error in adding details : ", ex, "value here si ", val, val1

except Exception,e:
    print "Error in reading website ", e
    sys.exit(0)
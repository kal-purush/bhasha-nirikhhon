#returns json of people you are not friends with,
#public groups you are not in

import webapp2
import os
import json

from google.appengine.ext.webapp import template
from google.appengine.api import users
from google.appengine.api import mail
from google.appengine.ext import ndb
from models.account import user_info
from models.group import Group
from models.group_members import GroupMembers
import logging

def render_template(handler, templatename, templatevalues):
    path = os.path.join(os.path.dirname(__file__), templatename)
    html = template.render(path, templatevalues)
    handler.response.out.write(html)


class SearchFriends(webapp2.RequestHandler):
    def get(self):
        user = users.get_current_user()
        info = user_info
        if user:
            info = user_info.get_user_account()
            friends = info.friend_list

            #list of all users
            all_users = user_info.query().order(user_info.email).fetch()
            #logging.info(all_users)
            result = []
            in_counter = 0
            for a_user in all_users:
                if in_counter < len(friends):
                    
                    if a_user.email == friends[in_counter]:
                        in_counter += 1
                    else:
                        logging.info("adding a non-friend "+ a_user.email)
                        result.append(a_user.email)
                        #add friend to list
                else:
                    logging.info("adding a non-friend "+ a_user.email)
                    result.append(a_user.email)
                     #at end of friend_list

            self.response.out.write(json.dumps(result))
            #self.response.out.write(template.render('templates/search.html', {
            #render_template(self, 'templates/search.html',{
            #    "friends": friends,
             #   "users": all_users,
              #  "non_friends": result
            #}))




class SearchGroups(webapp2.RequestHandler):
    def get(self):
        groups = Group.query().order(Group.name).fetch()
        group_list = []
        for group in groups:
            group_list.append({"name": group.name,"blurb": group.blurb})

        self.response.out.write(template.render('templates/search.html', {
            #render_template(self, 'templates/search.html',{
                "group_list": group_list
            }))

class SearchPage(webapp2.RequestHandler):
    def get(self):
        self.response.out.write(template.render('templates/search.html', {}))
# -*- coding: utf-8 -*-
from random import choice
from model.group import Group


def test_link_contact_to_group(app, db, orm, check_ui):
    app.contact.ensure_existence_sanity_check(db)
    app.group.ensure_existence_sanity_check(db)
    contact_to_link = choice(db.get_contact_list())
    group_to_link = choice(db.get_group_list())
    # if contact_to_link.id in (c.id for c in orm.get_contacts_in_group(group_to_link)):
    #     app.navigation.open_groups_page()
    #     app.group.create(Group(name="Safety Group"))
    app.navigation.go_home()
    app.contact.link_to_group(contact_to_link, group_to_link)
    assert contact_to_link.id in (c.id for c in orm.get_contacts_in_group(group_to_link))
    if check_ui:
        assert app.contact.details_has_group(contact_to_link, group_to_link)
import sqlalchemy
import os
from datetime import date, timedelta
from DBSetup import *
from sqlalchemy import *
from sqlalchemy.orm import aliased
from config import *
from SMTPemail import *

#Looks up the amount of times a user has participated in an "ismusical" group during the camp
def playcount(session,userid):
    playcount = session.query(user.userid).join(groupassignment, user.userid == groupassignment.userid).join(group, groupassignment.groupid == group.groupid).outerjoin(period).filter(user.userid == userid, group.ismusical == 1, period.endtime <= datetime.datetime.now()).count()
    return playcount

#get the information needed to fill in the user's schedule table
def getschedule(session,thisuser,date):
    #find the start of the next day
    nextday = date + datetime.timedelta(days=1)

    schedule = []
    #for each period in the requested day
    for p in session.query(period).filter(period.starttime > date, period.endtime < nextday).order_by(period.starttime).all():
        #try to find a group assignment for the user
        g = session.query(group.groupname, period.starttime, period.endtime, location.locationname, group.groupid, group.ismusical, group.iseveryone, music.musicid, \
                            period.periodid, period.periodname, groupassignment.instrumentname, period.meal, group.groupdescription, music.composer, music.musicname, group.musicwritein).\
                            join(period).join(groupassignment).join(user).join(instrument).outerjoin(location).outerjoin(music).\
                            filter(user.userid == thisuser.userid, group.periodid == p.periodid, group.status == 'Confirmed').first()
        if g is not None:
            schedule.append(g)
        e = None
        #if we didn't find an assigned group...
        if g is None:
            #try to find an iseveryone group at the time of this period
            e = session.query(group.groupname, period.starttime, period.endtime, location.locationname, group.groupid, group.ismusical, \
                            group.iseveryone, period.periodid, period.periodname, period.meal, group.groupdescription).\
                            join(period).join(location).\
                            filter(group.iseveryone == 1, group.periodid == p.periodid).first()
        if e is not None:
            schedule.append(e)
        #if we didn't find an assigned group or an iseveryone group...
        if e is None and g is None:
            #just add the period detalis
            schedule.append(p)
    return schedule

#from an input group, returns the highest and lowest instrument levels that should be assigned depending on its config
def getgrouplevel(session,inputgroup,min_or_max):
    if min_or_max == 'min':
        log('GETGROUPLEVEL: Finding group %s level' % min_or_max)
        #if the group is set to "auto", i.e. blank or 0, find the minimum level of all the players currently playing in the group
        if inputgroup.minimumlevel is None or inputgroup.minimumlevel == '' or inputgroup.minimumlevel == '0' or inputgroup.minimumlevel == 0:
            minimumgradeob = session.query(user.firstname, user.lastname, instrument.instrumentname, instrument.grade).join(groupassignment).join(group).\
                join(instrument, and_(groupassignment.instrumentname == instrument.instrumentname, user.userid == instrument.userid)).\
                filter(group.groupid == inputgroup.groupid).order_by(instrument.grade).first()
            #if we find at least one player in this group, set the minimumgrade to be this players level minus the autoassignlimitlow
            if minimumgradeob is not None:
                log('GETGROUPLEVEL: Found minimum grade in group %s to be %s %s playing %s with grade %s' % (inputgroup.groupid, minimumgradeob.firstname, minimumgradeob.lastname, minimumgradeob.instrumentname, minimumgradeob.grade))
                level = int(minimumgradeob.grade) - int(getconfig('AutoAssignLimitLow'))
                if level < 1:
                    level = 1
            #if we don't find a player in this group, set the minimum level to be 0 (not allowed to autofill)
            else: 
                level = 0
        #if this group's minimum level is explicitly set, use that setting
        else:
            level = inputgroup.minimumlevel
    if min_or_max == 'max':
        if inputgroup.maximumlevel is None or inputgroup.maximumlevel == '' or inputgroup.maximumlevel == '0' or inputgroup.maximumlevel == 0:
            minimumgradeob = session.query(user.firstname, user.lastname, instrument.instrumentname, instrument.grade).join(groupassignment).join(group).\
                join(instrument, and_(groupassignment.instrumentname == instrument.instrumentname, user.userid == instrument.userid)).\
                filter(group.groupid == inputgroup.groupid).order_by(instrument.grade).first()
            #if we find at least one player in this group, set the maximumgrade to be this players level plus the autoassignlimithigh
            if minimumgradeob is not None:
                log('GETGROUPLEVEL: Found minimum grade in group %s to be %s %s playing %s with grade %s' % (inputgroup.groupid, minimumgradeob.firstname, minimumgradeob.lastname, minimumgradeob.instrumentname, minimumgradeob.grade))
                level = int(minimumgradeob.grade) + int(getconfig('AutoAssignLimitHigh'))
                if level > int(getconfig('AutoAssignLimitHigh')):
                    level = getconfig('MaximumLevel')
            #if we don't find a player in this group, set the maximum level to 0 (not allowed to autofill)
            else:
                level = 0
        #if this group's maximum level is explicitly set, use that setting
        else:
            level = inputgroup.maximumlevel
    log('GETGROUPLEVEL: Found %s grade of group %s to be %s' % (min_or_max,inputgroup.groupid,level))
    return int(level)

def getgroupname(session,thisgroup):
    log('GETGROUPNAME: Generating a name for requested group')
    instrumentlist = getconfig('Instruments').split(",")

    #if this group's instrumentation matches a grouptempplate, then give it the name of that template
    templatematch = session.query(grouptemplate).filter(*[getattr(grouptemplate,i) == getattr(thisgroup,i) for i in instrumentlist]).first()
    if templatematch is not None:
        log('GETGROUPNAME: Found that this group instrumentation matches the template %s' % templatematch.grouptemplatename)
        name = templatematch.grouptemplatename

    #if we don't get a match, then we find how many players there are in this group, and give it a more generic name
    else:
        count = 0
        for i in instrumentlist:
            value = getattr(thisgroup, i)
            log('GETGROUPNAME: Instrument %s is value %s' % (i, value))
            if value is not None:
                count = count + int(getattr(thisgroup, i))
        log('GETGROUPNAME: Found %s instruments in group.' % value)
        if count == 1:
            name = 'Solo'
        elif count == 2:
            name = 'Duet'
        elif count == 3:
            name = 'Trio'
        elif count == 4:
            name = 'Quartet'
        elif count == 5:
            name = 'Quintet'
        elif count == 6:
            name = 'Sextet'
        elif count == 7:
            name = 'Septet'
        elif count == 8:
            name = 'Octet'
        elif count == 9:
            name = 'Nonet'
        elif count == 10:
            name = 'Dectet'
        else:
            name = 'Custom Group'

    if thisgroup.musicid is not None:
        log('GETGROUPNAME: Found that this groups musicid is %s' % thisgroup.musicid)
        composer = session.query(music).filter(music.musicid == thisgroup.musicid).first().composer
        log('GETGROUPNAME: Found composer matching this music to be %s' % composer)
        name = composer + ' ' + name
        
    log('GETGROUPNAME: Full name of group returned is %s' % name)
    return name

#iterates through the empty slots in a group and finds players to potentially fill them. returns a list of players.
def autofill(session,thisgroup,thisperiod,primary_only):
    #get the minimum level of this group
    minlevel = getgrouplevel(session,thisgroup,'min')
    maxlevel = getgrouplevel(session,thisgroup,'max')
    if minlevel == 0 or maxlevel == 0:
        raise NameError('You cannot autofill with all empty players and an auto minimum or maximum level. Set the level or pick at least one player.')
    final_list = []
    for i in getconfig('Instruments').split(","):
        numberinstrument = getattr(thisgroup,i)
        if int(numberinstrument) > 0:
            log('AUTOFILL: Group has configured %s total players for instrument %s' % (numberinstrument, i))
            currentinstrumentplayers = session.query(user).join(groupassignment).filter(groupassignment.groupid == thisgroup.groupid, groupassignment.instrumentname == i).all()
            requiredplayers = int(numberinstrument) - len(currentinstrumentplayers)
            log('AUTOFILL: Found %s current players for instrument %s' % (len(currentinstrumentplayers), i))
            log('AUTOFILL: We need to autofill %s extra players for instrument %s' % (requiredplayers, i))
            if requiredplayers > 0:
                #get the userids of everyone that's already playing in something this period
                everyone_playing_in_period_query = session.query(
                        user.userid.label('everyone_playing_in_period_query_userid')
                    ).join(groupassignment
                    ).join(group
                    ).join(period
                    ).filter(
                        period.periodid == thisgroup.periodid
                    )

                #combine the last query with another query, finding everyone that both plays an instrument that's found in thisgroup AND isn't in the list of users that are already playing in this period.
                #The admin controls if the query allows non-primary instruments by the primary_only dropdown on their UI
                possible_players_query = session.query(
                        user.userid.label('possible_players_query_userid')
                    ).outerjoin(instrument
                    ).filter(
                        ~user.userid.in_(everyone_playing_in_period_query), 
                        user.isactive == 1, 
                        user.arrival <= thisperiod.starttime, 
                        user.departure >= thisperiod.endtime
                    ).filter(
                        instrument.grade >= minlevel, 
                        instrument.grade <= maxlevel, 
                        instrument.instrumentname == i, 
                        instrument.isactive == 1, 
                        instrument.isprimary >= int(primary_only)
                    )

                log('AUTOFILL: Found %s possible players of a requested %s for instrument %s.' % (len(possible_players_query.all()), requiredplayers, i))
                #if we found at least one possible player
                if len(possible_players_query.all()) > 0:
                    #get the players that have already played in groups at this camp, and inverse it to get the players with playcounts of zero. Limit the query to just the spots we have left. These will be the people at the top of the list.
                    already_played_query = session.query(
                            user.userid
                        ).join(groupassignment
                        ).join(group
                        ).filter(
                            group.ismusical == 1, 
                            groupassignment.instrumentname != 'Conductor', 
                            user.userid.in_(possible_players_query)
                        )

                    #make a query that totals the nmber of times each potential user has played at camp.
                    playcounts_subquery = session.query(
                            user.userid.label('playcounts_userid'),
                            func.count(user.userid).label("playcount")
                        ).group_by(
                            user.userid
                        ).outerjoin(groupassignment, groupassignment.userid == user.userid
                        ).outerjoin(group, group.groupid == groupassignment.groupid
                        ).filter(
                            user.userid.in_(possible_players_query),
                            groupassignment.instrumentname != 'Conductor', 
                            group.ismusical == 1, 
                            group.periodid != None,
                            group.groupid != thisgroup.groupid
                        ).subquery()

                    #if this group has assigned music
                    if thisgroup.musicid is not None:
                        #get a count of the amount of times that each potential player has played in this group
                        musiccounts_subquery = session.query(
                                user.userid.label('musiccounts_userid'),
                                func.count(user.userid).label("musiccount")
                            ).group_by(
                                user.userid
                            ).outerjoin(groupassignment, groupassignment.userid == user.userid
                            ).outerjoin(group, group.groupid == groupassignment.groupid
                            ).filter(
                                user.userid.in_(possible_players_query),
                                groupassignment.instrumentname != 'Conductor', 
                                group.ismusical == 1,           
                                group.periodid != None,
                                group.groupid != thisgroup.groupid,
                                #grab all groups that share a musicid with this group
                                group.musicid == thisgroup.musicid
                            ).subquery()
                    else:
                        #make a query with all userids, and 0s in the next column so the subsequent queries work (because no users have a playcount for this music)
                        musiccounts_subquery = session.query(
                                user.userid.label('musiccounts_userid'),
                                sqlalchemy.sql.expression.literal_column("0").label("musiccount")
                            ).filter(
                                user.userid.in_(possible_players_query),
                            ).subquery()

                    #make a query we can use to total up the number of times each user has played in a group like this
                    groupcounts_subquery = session.query(
                            user.userid.label('groupcounts_userid'),
                            func.count(user.userid).label("groupcount")
                        ).group_by(
                            user.userid
                        ).outerjoin(groupassignment, groupassignment.userid == user.userid
                        ).outerjoin(group, group.groupid == groupassignment.groupid
                        ).filter(
                            user.userid.in_(possible_players_query),
                            groupassignment.instrumentname != 'Conductor', 
                            group.ismusical == 1,           
                            group.periodid != None,
                            group.groupid != thisgroup.groupid,
                            #grab all groups that share a name with this group, or have the same instrumentation of this group
                            or_(
                                group.groupname == thisgroup.groupname,
                                and_(*[getattr(thisgroup,inst) == getattr(group,inst) for inst in instrumentlist])
                            )
                        ).subquery()

                    #Put it all together!
                    instrument_list = session.query(
                            user.userid,
                            user.firstname,
                            user.lastname,
                            sqlalchemy.sql.expression.literal(i).label("instrumentname"),
                            playcounts_subquery.c.playcount,
                            musiccounts_subquery.c.musiccount,
                            groupcounts_subquery.c.groupcount
                        ).outerjoin(playcounts_subquery, playcounts_subquery.c.playcounts_userid == user.userid
                        ).outerjoin(musiccounts_subquery, musiccounts_subquery.c.musiccounts_userid == user.userid
                        ).outerjoin(groupcounts_subquery, groupcounts_subquery.c.groupcounts_userid == user.userid
                        ).filter(
                            user.userid.in_(possible_players_query)
                        ).order_by(
                            #order by the number of times that the players have played this specific piece of music
                            musiccounts_subquery.c.musiccount.nullsfirst(), 
                            #then order by the number of times that the players have played in a group similar to this in name or instrumentation
                            groupcounts_subquery.c.groupcount.nullsfirst(), 
                            #then order by their total playcounts
                            playcounts_subquery.c.playcount.nullsfirst()
                        ).limit(requiredplayers
                        ).all()
                    log('AUTOFILL: Players in final list with playcounts:')
                    for pl in instrument_list:
                        log('AUTOFILL: Found %s %s to play %s with totals - musiccount:%s groupcount:%s playcount:%s ' % (pl.firstname, pl.lastname, pl.instrumentname, pl.musiccount, pl.groupcount, pl.playcount))
                        final_list.append(pl)
    return final_list
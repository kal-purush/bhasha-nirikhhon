from __future__ import print_function
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, RegexHandler, InlineQueryHandler
import logging
import os
import subprocess
import re
from telegram import InlineQueryResultArticle, ChatAction, InputTextMessageContent
import time
import random
import json
import requests
import datetime
from datetime import datetime, timedelta
import configparser
import time
import datetime
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import wikipedia

conf = configparser.RawConfigParser()
conf.read('eva.ini')
token = conf['TOKENS']['tg_api']
userid = int(conf['USER']['userid'])
#noviaid = int(conf['USER']['noviaid']
weather_api = conf['TOKENS']['accu_api']
todopath = conf['PATHS']['todo']
notepath = conf['PATHS']['notebook']
logfile = conf['PATHS']['logfile'] 
simon = conf['NAMES']['admin']
novia = conf['NAMES']['novia']
dailyjob_hour = int(conf['DAILYMESSAGE']['hour'])
dailyjob_min = int(conf['DAILYMESSAGE']['min'])
google_cal_json = conf['PATHS']['google_cal_json']
google_cal_pickle = conf['PATHS']['google_cal_pickle']
#noviastate = conf['FEATURES']['novia_on_off']


# logging
logging.basicConfig(filename=logfile,level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# basic command handlers
def start(bot, update):
	update.message.reply_text('Estoy viva!')
	logging.info('Bot started')    	

def help(bot, update):
	update.message.reply_text('Help!')


# typing action
def typing(bot, update):
	bot.sendChatAction(chat_id=update.message.chat_id, action=ChatAction.TYPING)


# obtaining google api calendar credencials
def calendarcred():

	# If modifying these scopes, delete the file token.pickle.
	SCOPES = ['https://www.googleapis.com/auth/calendar.events']

	creds = None
	# The file token.pickle stores the user's access and refresh tokens, and is
	# created automatically when the authorization flow completes for the first
	# time.
	if os.path.exists(google_cal_pickle):
		with open(google_cal_pickle, 'rb') as token:
			creds = pickle.load(token)
	# If there are no (valid) credentials available, let the user log in.
	if not creds or not creds.valid:
		logging.info('Generating google calendar credentials..')
		if creds and creds.expired and creds.refresh_token:
			creds.refresh(Request())
		else:
			flow = InstalledAppFlow.from_client_secrets_file(google_cal_json, SCOPES)
			creds = flow.run_local_server()
		# Save the credentials for the next run
		with open(google_cal_pickle, 'wb') as token:
			pickle.dump(creds, token)

	return creds 

# query the google calendar api to obtain X amounts of events
def lastevents(maxdays, calendarid):
	
	creds = calendarcred()
	service = build('calendar', 'v3', credentials=creds)
	# Call the Calendar API
	now = datetime.datetime.utcnow().isoformat() + 'Z' # 'Z' indicates UTC time
	limit = datetime.datetime.utcnow() + datetime.timedelta(days=maxdays) 
	limit = limit.isoformat() + 'Z'
	events_result = service.events().list(calendarId=calendarid, timeMin=now, maxResults=15, singleEvents=True, orderBy='startTime', timeMax=limit).execute()
	events = events_result.get('items', [])
                #print(events)

	if not events:
		events = 'Lo siento, no encontre ningun evento en los proximos 15 dias.'
	else:
		listt = list()
		for event in events:
			start = event['start'].get('dateTime', event['start'].get('date'))   #
			print(start)
			start = str(start)[6:10]
			print(start)
			events = start + " " + str(event['summary']) #[0:25]) + "..."
			print(events)
			events = events + "\n" + "\n"
			listt.append(events)
		listt = ''.join(listt)
		events = listt

	return events

# command to add event to the primary calendar
def eventadd(bot, update, args):

	creds = calendarcred()
	service = build('calendar', 'v3', credentials=creds)
	fecha = args[0]
	print(fecha)
	fecha1 = fecha + 'T09:00:00-03:00'
	fecha2 = fecha + 'T10:00:00-03:00'
	print(fecha1)
	print(fecha2)
	summary = " ".join(args)
	summary = summary[10:-1]		
	start = {'dateTime':fecha1}
	end = {'dateTime':fecha2}
	payload = {'summary':summary, 'start':start, 'end':end}
	insert = service.events().insert(calendarId='primary', body=payload, sendNotifications=None, supportsAttachments=None, sendUpdates=None, conferenceDataVersion=None, maxAttendees=None)

	insert.execute()
	logging.info('Google Calendar event added')

# function to read X file
def read(path):

	f = open(path, "r")
	content = f.read() 
	f.close()
	return content

# function to append text to X file
def append(path, content):

	f = open(path, "a")
	f.write("-" + content + "\n")
	f.close()

#function to delete line from X file
def delete(path, content):
	
	notok = "error"
	ok = "ok"
	f = open(path, "r")
	filetext = f.read()
	match3 = re.search(content, str(filetext))
	
	if (match3):
		with open(path, "r") as f:
    			lines = f.readlines()
		with open(path, "w") as f:
			for line in lines:
				if line.strip("\n") != "-" + content:
					f.write(line)
		# if the loop runs return ok
		return ok
		
	else: 
		# else return error
		return notok

# execute commands on running machine
def execute(bot, update, args):

	try:
		user_id = update.message.from_user.id
		command = ' '.join(args)
		
		inline = False
	except AttributeError:
		# Using inline
		user_id = update.inline_query.from_user.id
		command = update.inline_query.query
		inline = True

	if user_id == userid:
		print(command)
		print(args)
		if not inline:
			bot.sendChatAction(chat_id=update.message.chat_id, action=ChatAction.TYPING)
			bot.sendMessage(chat_id=update.message.chat_id, text= simon + ", el output de tu comando es...")
			bot.sendChatAction(chat_id=update.message.chat_id, action=ChatAction.TYPING)
			output = subprocess.Popen(command, shell=True, executable='/bin/bash', stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
			output = output.stdout.read().decode('utf-8')
			output = '`{0}`'.format(output)
			logging.info('Command \'%s\' executed', command)

		if not inline:
			bot.sendMessage(chat_id=update.message.chat_id, text=output, parse_mode="Markdown")
			return False

		if inline:
			return output
	else:
		bot.sendMessage(chat_id=update.message.chat_id, text="Disculpa, pero solo " + simon + " puede usarme para ejecutar comandos en el servidor donde vivo actualmente, le hare saber lo que estas intentando hacer!")
		bot.sendMessage(chat_id=userid, text="Simon! alguien esta tratando de usarme sin tu permiso!")
		print(user_id)
		logging.warning('Unauthorized command execution attempt, userid: %s ', user_id)

# command: append elements in the todo.txt file using the append() func
def todoadd(bot, update, args):
	
	args = ' '.join(args)
	user_id = update.message.from_user.id
	if user_id == userid:
		append(todopath, args)
		typing(bot, update)
		bot.sendMessage(chat_id=update.message.chat_id, text='Se agrego "' + args + '" a tus tareas')
		logging.info('Element added in %s ', todopath) 

# command: delete lines in the todo.txt file using the delete() func
def deletetodo(bot, update, args):

	args = ' '.join(args)
	user_id = update.message.from_user.id
	if user_id == userid:
		status = delete(todopath, args)
		delete(todopath, args)
		# testing if the loop runs
		if status == "ok":
			typing(bot, update)
			bot.sendMessage(chat_id=update.message.chat_id, text='Se elimino "' + args + '" de tus tareas')
			logging.info('Element deleted in %s ', todopath)
		else:
			typing(bot, update)
			bot.sendMessage(chat_id=update.message.chat_id, text='Lo siento, el elemento "' + args + '" no existe en la lista')
			

# command: append elements in the note.txt file using the append() func
def noteadd(bot, update, args):

	args = ' '.join(args)
	user_id = update.message.from_user.id
	if user_id == userid:
		append(notepath, args)
		typing(bot, update)
		bot.sendMessage(chat_id=update.message.chat_id, text='Se agrego "' + args + '" a tus anotaciones') 
		logging.info('Element added in %s ', notepath)

# command: delete elements in the note.txt file using the delete() func
def deletenote(bot, update, args):

	args = ' '.join(args)
	user_id = update.message.from_user.id
	if user_id == userid:
		status = delete(notepath, args)
		delete(notepath, args)
		# testing if the loop runs
		if status == "ok":
			typing(bot, update)
			bot.sendMessage(chat_id=update.message.chat_id, text='Se elimino "' + args + '" de tus anotaciones')
			logging.info('Element deleted in %s ', notepath)
		else:
			typing(bot, update)
			bot.sendMessage(chat_id=update.message.chat_id, text='Lo siento, el elemento "' + args + '" no existe en la lista')

# daily message configured by the .ini conf file
def bonjour(bot, job):
	
	response = requests.get(weather_api)
	loaded = json.loads(response.text)
	loaded = loaded['DailyForecasts'][0]
	rainday = str(loaded['Day']['RainProbability'])
	rainnight1 = str(loaded['Night']['RainProbability'])
	tempday1 = str(loaded['Temperature']['Maximum']['Value'])
	tempnight1 = str(loaded['Temperature']['Minimum']['Value'])

	# iftemp() and ifrain() to build the string with weather information
	def iftemp(interger):

		if float(interger) < float(18):
			frase = "abrigate! el dia estara a " + interger + "C"
		else:
			frase = "no te tienes que abrigar, el dia estara a " + interger + "C"
		return frase

	def ifrain(interger):
	
		if float(interger) > float(45):
			frase = "deberias llevar paraguas y botas, ya que la probabilidad de lluvia es de  " + interger + "%"
		else:
			frase = "no parece que fuera a llover, la probabilidad de lluvia es de " + interger + "%"
		return frase

	frase1 = iftemp(tempday1)
	frase2 = ifrain(rainday)
	bonjourstr = "Buen dia " + simon + "! hoy " + frase1 + " y " + frase2
	todo = read(todopath)
	bot.sendChatAction(chat_id=userid, action=ChatAction.TYPING)
	bot.send_message(chat_id=userid, text=bonjourstr)
	bot.sendChatAction(chat_id=userid, action=ChatAction.TYPING)
	bot.send_message(chat_id=userid, text= "tus tareas pendientes son...")
	bot.sendChatAction(chat_id=userid, action=ChatAction.TYPING)
	bot.send_message(chat_id=userid, text=todo)
	calendar1 = lastevents(int(1), 'primary')
	calendar2 = lastevents(int(1), 'simongmalav@gmail.com')
	bot.sendChatAction(chat_id=userid, action=ChatAction.TYPING)
	bot.send_message(chat_id=userid, text= "y tus eventos de hoy son...")

	def dayevents(calvariable, typevariable, underline):
		if calvariable != 'Lo siento, no encontre ningun evento en los proximos 15 dias.':
			bot.sendChatAction(chat_id=userid, action=ChatAction.TYPING)
			bot.send_message(chat_id=userid, text=typevariable + '\n' + underline + '\n' + calvariable)
		else:
			bot.sendChatAction(chat_id=userid, action=ChatAction.TYPING)
			bot.send_message(chat_id=userid, text=typevariable + '\n' + underline + '\n' + "para el dia de hoy no tienes eventos pendientes.")

	dayevents(calendar1, 'Trabajo:', '------------')
	dayevents(calendar2, 'Eventos personales:', '------------------------------')
	logging.info('Daily message executed')
	#if noviastate == True:
	#	bonjourstr = "Buen dia " + novia + "! hoy " + frase1 + " y " + frase2
	#	bot.sendChatAction(chat_id=noviaid, action=ChatAction.TYPING)
	#	bot.send_message(chat_id=noviaid, text=bonjourstr)

	
# return string to respond to climate querys in the noncommand event reactions
def clima(full_or_not):

	def message(number, hoy):

		response = requests.get(weather_api)
		loaded = json.loads(response.text)
		loaded = loaded['DailyForecasts']
		# day 
		day = loaded[number]
		date = str(day['Date'])[0:10]
		dt = datetime.datetime.strptime(date, '%Y-%m-%d')
		weekday = dt.strftime('%a')
		tempday = str(day['Temperature']['Maximum']['Value'])
		tempnight = str(day['Temperature']['Minimum']['Value'])
		rainday = str(day['Day']['RainProbability'])
		rainnight = str(day['Night']['RainProbability'])
		dict = {'Mon':'Lunes', 'Tue':'Martes', 'Wed':'Miercoles', 'Thu':'Jueves', 'Fri':'Viernes', 'Sat':'Sabado', 'Sun':'Domingo'}
		daymesg = hoy + dict[weekday] + ": \n \n" + "Dia: \n=== \nTemperatura: " + tempday + "C. Probabilidad de lluvia: " + rainday + "% \n \n" + "Noche: \n===== \nTemperatura: " + tempnight + "C. Probabilidad de lluvia: " + rainnight + "% \n \n"

		return daymesg
	
	day1 = message(0, "Hoy, ")
	day2 = message(1, "Mañana, ")
	day3 = message(2, "Pasado mañana, ")
	day4 = message(3, "")
	day5 = message(4, "")
	
	if full_or_not == "full":
		finalstr = day1 + day2 + day3 + day4 + day5
	else:
		finalstr = day1 #+ day2 + day3

	return finalstr


# noncommand events reactions
def ifbot(bot, update):
	
	message = str(update.message.text)
#	stringclima = "El Clima es:" + output1
	match11 = re.search("(?i)clima completo|(?i)tiempo completo", str(message))
	match1 = re.search("(?i)clima|(?i)tiempo", str(message))
	# finance command
	match2 = re.search("(?i)dinero|(?i)lucas", str(message))
	answer2 = [simon + ', tu dinero actual es:', simon + ', en tu cuenta te quedan..', simon + ", tu balance es.."]
	answer2 = random.choice(answer2)
	# todo command
	match3 = re.search("(?i)tareas|(?i)pendientes", str(message))
	# note command
	match4 = re.search("(?i)anotaciones|(?i)cuaderno", str(message))
	# calendar
	match5 = re.search("(?i)agenda|(?i)calendario|(?i)eventos", str(message))	
	# wiki
	#wikianswer = wiki(message)
	# unknown command
	noent = update.message.text + "? ... lo siento, no estoy programada para entender eso"
	user_id = update.message.from_user.id

	if user_id == userid:
		if (match11):
			typing(bot, update)
			update.message.reply_text(simon + ", el clima en los proximos 5 dias sera... ")
			stringclima = clima("full")
			update.message.reply_text(stringclima)
		elif (match1):
			typing(bot, update)
			update.message.reply_text(simon + ", el clima... ")
			stringclima = clima("")
			update.message.reply_text(stringclima)

		elif (match2):
			typing(bot, update)
			update.message.reply_text(answer2)
		elif (match3):
			typing(bot, update)
			todo = read(todopath)
			update.message.reply_text(simon + ", tus tareas pendientes son...")
			typing(bot, update)
			update.message.reply_text(todo)
		elif (match4):
			typing(bot, update)
			notes = read(notepath)
			update.message.reply_text(simon + ", tus anotaciones son...")
			typing(bot, update)
			update.message.reply_text(notes)
		elif (match5):
			typing(bot, update)
			update.message.reply_text('Obteniendo eventos de los proximos 15 dias..')
			typing(bot, update)
			calendar1 = lastevents(int(15),'primary')
			calendar2 = lastevents(int(15),'simongmalav@gmail.com')
			update.message.reply_text( 'Trabajo:' + '\n' + '------------' + '\n' + calendar1 + 'Eventos personales:' + '\n' + '------------------------------' + '\n' + calendar2)
		elif wiki(message) != False:
			typing(bot, update)
			wikianswer = wiki(message)
			update.message.reply_text(wikianswer)

		else:
			typing(bot, update)
			update.message.reply_text(noent)
		logging.info('Conversation handler executed')
	
	else:
		bot.sendMessage(chat_id=update.message.chat_id, text="Lo siento, no estoy autorizada para hablar contigo, comunicate con @malavz para mas informacion ")
		user = update.message.from_user['username']
		bot.sendMessage(chat_id=userid, text="Simon! @" + str(user) + "  esta tratando de usarme sin tu permiso!")
		print(user_id)
		logging.warning('Unauthorized conversation attempt, userid: %s ', user_id)
	



# maximum message length is 4096 UTF8 characters
# Wikipedia query
def wiki(query):

	wikipedia.set_lang("es")
	try:
		ans = (wikipedia.summary(query))
		ans = re.sub("[\[].*?[\]]", '', ans)
		url = wikipedia.page(query).url
		if len(ans) > 3900:
			ans = ans[0:3900] + "..." + "\n \n" + "Continuar leyendo en: " + url
		else:
			ans = ans[0:3900]
	except wikipedia.exceptions.DisambiguationError as e:
		ans = False
		print('Wikipedia DisambiguationError')
	except wikipedia.exceptions.PageError as e:
		ans = False
		print('Wikipedia Page not found')

	return ans


def error(bot, update, error):
	"""Log Errors caused by Updates."""
	logger.warning('Update "%s" caused error "%s"', update, error)


def main():

		
	
	"""Start the bot."""
	# Create the EventHandler and pass it your bot's token.
	updater = Updater(token) 
	j = updater.job_queue
	# Get the dispatcher to register handlers
	dp = updater.dispatcher



	# on different commands - answer in Telegram
	dp.add_handler(CommandHandler("start", start))
	dp.add_handler(CommandHandler("help", help))
	dp.add_handler(CommandHandler("execute", execute, pass_args=True))
	dp.add_handler(CommandHandler("todo", todoadd, filters=Filters.chat(chat_id=userid), pass_args=True))
	dp.add_handler(CommandHandler("deletetodo", deletetodo, filters=Filters.chat(chat_id=userid),  pass_args=True))
	dp.add_handler(CommandHandler("noteadd", noteadd, filters=Filters.chat(chat_id=userid), pass_args=True))
	dp.add_handler(CommandHandler("deletenote", deletenote, filters=Filters.chat(chat_id=userid), pass_args=True))
	dp.add_handler(CommandHandler("eventadd", eventadd, filters=Filters.chat(chat_id=userid), pass_args=True))	
	
	# daily message
	job_minute = j.run_daily(bonjour, datetime.time(hour=dailyjob_hour, minute=dailyjob_min))
	
	# on noncommand i.e message - echo the message on Telegram
	dp.add_handler(MessageHandler(Filters.text, ifbot))

	# log all errors
	dp.add_error_handler(error)

	# Start the Bot
	updater.start_polling()

	# Run the bot until you press Ctrl-C or the process receives SIGINT,
	# SIGTERM or SIGABRT. This should be used most of the time, since
	# start_polling() is non-blocking and will stop the bot gracefully.
	updater.idle()



if __name__ == '__main__':
	main()
import sys
import time
import os
import logging
import sqlite3
from watchdog.observers import Observer, polling
#from watchdog.events import LoggingEventHandler
from watchdog.events import FileSystemEventHandler
from watchdog.observers.polling import PollingObserver


class EventHandler(FileSystemEventHandler):
    info = []
    conn = sqlite3.connect('/home/pi/pimame/pimame-menu/database/config.db')
    c = conn.cursor()
    
    def on_any_event(self, event):

        if not event.is_directory:
            if event.event_type == 'created': 
                self.info.append(self.c.execute('SELECT id FROM menu_items WHERE rom_path LIKE "{0}%"'.format(os.path.dirname(event.src_path))).fetchone()[0])
                print 'new file found:', event.src_path
                if event.event_type == 'deleted':
                self.info.append(self.c.execute('SELECT id FROM menu_items WHERE rom_path LIKE "{0}%"'.format(os.path.dirname(event.src_path))).fetchone()[0])
                print 'file deleted:', event.src_path
	else: print

if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else '.'
    event_handler = EventHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(60)
            if event_handler.info: os.system('python /home/pi/pimame/pimame-menu/scraper/scrape_script.py --platform {0}'.format(str(set(event_handler.info))[5:-2].replace(' ','')))
            event_handler.info = []
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

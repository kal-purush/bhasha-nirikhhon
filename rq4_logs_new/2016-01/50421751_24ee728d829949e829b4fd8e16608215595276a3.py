#!/usr/bin/env python
__author__ = "Bart van den Ende"

import re
import os
import sys
import lxml.etree as ET
import datetime
import multiprocessing as mp
import requests

class SmoothStreamCaptionExtractor:
    def __init__(self, stream):
        self.manifestUrl = stream
        self.basePath = os.path.dirname(stream)
        self.textTracks = []
        self.manifest = None
        self.timescale = 10000000
    
    def run(self):
        self.parse_main_manifest()
        for track in self.textTracks:
            self.parse_text_track(track)
        
    def parse_main_manifest(self):
        r = requests.get(self.manifestUrl)
        if r.status_code == requests.codes.ok:
            self.manifest = ET.fromstring(r.content)
            self.textTracks = self.manifest.xpath('.//StreamIndex[@Type = "text"]')
            if self.manifest.get('TimeScale'):
                self.timescale = self.manifest.get('TimeScale')
            print " loading manifest: 100%"
        else:
          sys.exit("failed to connect to '%s' with status code '%s'" % (self.manifestUrl, str(r.status_code)))
    
    def parse_text_track(self, track):
        track = TrackExtractor(self.basePath, self.timescale, track)
        track.parse()
        track.save()


class TrackExtractor:     
    def __init__(self, base_path, timescale, track):
        self.basePath = base_path
        self.title = os.path.basename(base_path)
        self.track = track
        self.language = "eng"
        self.bitrate = "128000"
        self.start_times = []
        self.last_chunk_duration = None
        self.ttmlmerger = TTMLMerger(timescale)
        self.url = None
      
    def parse(self):
        # get track language
        if self.track.get('Language'): 
            self.language = self.track.get('Language')
            
        # get fragments path
        if self.track.get('Url'): 
            self.url = self.basePath + "/" + self.track.get('Url')
            
        # get bitrate
        quality_levels = self.track.xpath('.//QualityLevel')
        if len(quality_levels) > 0: 
            self.bitrate = quality_levels[0].get("Bitrate")
            
        # parse start_times (fragments)
        for fragment in self.track.xpath('.//c'):
            self.parse_fragment_start_tag(fragment)
        
        # generate urls
        urls = []
        for start_time in self.start_times:
            urls.append(self.url.replace('{bitrate}', self.bitrate).replace('{start time}', str(start_time)))
        
        # fetch and save fragments
        pool = mp.Pool(processes=5)
        for i, fragment in enumerate(pool.imap(request_fragment, urls), 1):
            index = i - 1
            self.ttmlmerger.append(self.start_times[index], fragment)
            sys.stdout.write("\r loading fragments [%s]: %d%%" % (self.language, int((float(i)/len(self.start_times))*100)))
            sys.stdout.flush()
            
    def parse_fragment_start_tag(self, fragment):
        chunk_index = len(self.start_times)
        start_time = fragment.get("t")
        if not start_time:
            if chunk_index is 0:
                start_time = 0
            elif self.last_chunk_duration:
                start_time = self.start_times[chunk_index - 1] + self.last_chunk_duration
        chunk_index += 1
        self.start_times.append(start_time)
        self.last_chunk_duration = long(fragment.get("d"))
        # Handle repeated chunks.                             
        repeat_count = int(fragment.get("r")) if fragment.get("r") else 0
        for index in range(1, repeat_count-1):
            chunk_index += 1
            self.start_times.append(start_time + (self.last_chunk_duration * index))
        
    def save(self): 
        # merge ttml data
        ttml = self.ttmlmerger.build()
        
        # create dir if it does not already exits
        try:
            os.stat(self.title)
        except:
            os.mkdir(self.title)   
        
        # write file
        filename = self.title + os.sep + self.language + ".ttml" 
        f = open(filename, 'w')
        f.write(ttml)
        f.close()
        print "\n write ttml: %s" % (filename)


def request_fragment(url):
    r = requests.get(url)
    if r.status_code == requests.codes.ok:
        # cleanup response from any binary prefixes
        split_content = re.split('\<\?xml',r.content,1)
        if len(split_content) > 1:
            return "<?xml" + split_content[1]
        else:
            return r.content
    else:
      sys.exit("failed to connect to '%s' with status code '%s'" % (url, str(r.status_code)))


class TTMLMerger: 
    def __init__(self, timescale):
        self.timescale = timescale
        self.document = None
        self.subtitles = []
        self.CLOCK_TIME = re.compile("^([0-9][0-9]+):([0-9][0-9]):([0-9][0-9])"
          + "(?:(\\.[0-9]+)|:([0-9][0-9])(?:\\.([0-9]+))?)?$")
        self.OFFSET_TIME = re.compile("^([0-9]+(?:\\.[0-9]+)?)(h|m|s|ms|f|t)$")
        self.DEFAULT_FRAME_RATE = 30
        self.DEFAULT_SUBFRAME_RATE = 1
        self.DEFAULT_TICK_RATE = 1
        self.MICROS_PER_SECOND = 1000000

    def append(self, start_time, fragment):
        # make sure any whitespace from xml are removed so we can pretty print
        parser = ET.XMLParser(remove_blank_text=True)
        
        # store the main ttml document
        if self.document is None:
            clean_root = ET.fromstring(fragment, parser)
            clean_root.find('{http://www.w3.org/ns/ttml}body').clear()
            self.document = clean_root
        
        # get subtitle content out of xml
        root = ET.fromstring(fragment, parser)
        body = root.find('{http://www.w3.org/ns/ttml}body')
        content = body.getchildren()
        
        if len(content) > 0:
            # remove wrapping div
            if content[0].tag == '{http://www.w3.org/ns/ttml}div':
                content = content[0].getchildren()  
            
            # parse and correct time tags of the subtitles
            for el in content:
                self.parse_el(el, start_time)
                
            # store subtitle
            self.subtitles.insert(start_time, content)
            
    def parse_el(self, el, fragment_start_time):
        if el.tag != "{http://www.w3.org/ns/ttml}p": return
        
        # normalize fragment start time
        fragment_start_time_us = self.scale_large_timestamp(fragment_start_time, self.MICROS_PER_SECOND, self.timescale)

        # parse begin timestamp
        begin = el.get("begin")
        if begin: 
            # convert to MS
            start_time = self.parse_time_expression(begin, self.DEFAULT_FRAME_RATE, self.DEFAULT_SUBFRAME_RATE, self.DEFAULT_TICK_RATE)
            # add fragment start time, make time relative to beginning of file instead of to beginning of fragment
            start_time += fragment_start_time_us
            # convert back to timestamp
            el.set("begin", self.stringify_time_expression(start_time))
            
        # parse end timestamp
        end = el.get("end")
        if end: 
            # convert to MS
            end_time = self.parse_time_expression(end, self.DEFAULT_FRAME_RATE, self.DEFAULT_SUBFRAME_RATE, self.DEFAULT_TICK_RATE)
            # add fragment start time, make time relative to beginning of file instead of to beginning of fragment
            end_time += fragment_start_time_us
            # convert back to timestamp
            el.set("end", self.stringify_time_expression(end_time))
        
    def build(self):
        # create div and append subtitles
        div = ET.Element('{http://www.w3.org/ns/ttml}div')
        for subtitle in self.subtitles:
            for el in subtitle:
                div.append(el)
             
        # insert div in body
        body = self.document.find('{http://www.w3.org/ns/ttml}body')
        body.append(div)
        
        # remove any comments
        ET.strip_tags(body,ET.Comment)
        
        # create xml string
        xml = '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(self.document, pretty_print=True)
        
        # reset values
        body.clear() 
        
        return xml
    
    def parse_time_expression(self, time, frame_rate, subframe_rate, tick_rate):
        matcher = self.CLOCK_TIME.match(time)
        if len(matcher.groups()) > 0:
            hours = matcher.group(1)
            duration_seconds = long(hours) * 3600
            minutes = matcher.group(2)
            duration_seconds += long(minutes) * 60
            seconds = matcher.group(3)
            duration_seconds += long(seconds)
            fraction = matcher.group(4)
            duration_seconds += float(fraction) if fraction else 0
            frames = matcher.group(5)
            duration_seconds += long(frames) / frame_rate if frames else 0
            subframes = matcher.group(6)
            duration_seconds += long(subframes) / subframe_rate / frame_rate if subframes else 0
            return long(duration_seconds * self.MICROS_PER_SECOND)
        
        matcher = self.OFFSET_TIME.match(time)
        if len(matcher.groups()) > 0:
            time_value = matcher.group(1)
            offset_seconds = float(time_value)
            unit = matcher.group(2)
            if unit == "h":
                offset_seconds *= 3600
            elif unit == "m":
                offset_seconds *= 60
            # elif unit == "s":
                # Do nothing.
            elif unit == "ms":
                offset_seconds /= 1000
            elif unit == "f":
                offset_seconds /= frame_rate
            elif unit == "t":
                offset_seconds /= tick_rate
            return long(offset_seconds * self.MICROS_PER_SECOND)
    
    def stringify_time_expression(self, timestamp):
        timestamp = long(timestamp)
        milliseconds = (timestamp / 1000) % 1000
        seconds = (((timestamp / 1000) - milliseconds) / 1000) % 60
        minutes = (((((timestamp / 1000) - milliseconds) / 1000) - seconds) / 60) % 60
        hours = ((((((timestamp / 1000) - milliseconds) / 1000) - seconds) / 60) - minutes) / 60
        return "%.2d:%.2d:%.2d.%.3d" % (hours, minutes, seconds, milliseconds)
        
    def scale_large_timestamp(self, timestamp, multiplier, divisor):
        if divisor >= multiplier and (divisor % multiplier) == 0:
            division_factor = divisor / multiplier
            return timestamp / division_factor
        elif divisor < multiplier and (multiplier % divisor) == 0:
            multiplication_factor = multiplier / divisor
            return timestamp * multiplication_factor
        else:
            multiplication_factor = multiplier / divisor
            return timestamp * multiplication_factor

if __name__ == "__main__":
    if len(sys.argv) == 1:
        sys.exit("Not enough arguments")
    extractor = SmoothStreamCaptionExtractor(sys.argv[1])
    extractor.run()
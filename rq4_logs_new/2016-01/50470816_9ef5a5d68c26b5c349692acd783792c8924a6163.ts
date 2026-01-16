/// <reference path="../tools/typings/node/node.d.ts" />
import Config = require('./utils/ParseConfig');
import Song = require('./Song');

var config = new Config();

// Youtube url regex patterns (thanks to Matthieu, creator of mumbledj)
var youtubePlaylistPattern: string = 'https?:\/\/www\.youtube\.com\/playlist\?list=([\w-]+)';
var youtubeVideoPatterns: string[] = [
	'https?:\/\/www\.youtube\.com\/watch\?v=([\w-]+)(\&t=\d*m?\d*s?)?',
	'https?:\/\/youtube\.com\/watch\?v=([\w-]+)(\&t=\d*m?\d*s?)?',
	'https?:\/\/youtu.be\/([\w-]+)(\?t=\d*m?\d*s?)?',
	'https?:\/\/youtube.com\/v\/([\w-]+)(\?t=\d*m?\d*s?)?',
	'https?:\/\/www.youtube.com\/v\/([\w-]+)(\?t=\d*m?\d*s?)?',
];

function Fetch(): Song {
    
}

export = Fetch;
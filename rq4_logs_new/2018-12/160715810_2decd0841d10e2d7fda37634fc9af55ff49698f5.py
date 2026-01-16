#!/usr/bin/python3 -tt

import urllib.parse

def main():
	url = 'https://www.google.co.in/search?q=javatpoint&oq=javatpoint&aqs=chrome..69i57.4791j0j1&sourceid=chrome&ie=UTF-8'
	print(url)
	parsed = urllib.parse.urlparse(url)
	split1 = urllib.parse.urlparse(url)
	
	print('scheme (parse) :',parsed.scheme)
	print('scheme (split) :',split1.scheme)
	
	print('netloc (parse) :',parsed.netloc)
	print('netloc (split) :',split1.scheme)
	
	print('path (parse) :',parsed.path)
	print('path (split) :',split1.path)
	
	print('params (parse) :',parsed.params)
	print('params (split) :',split1.params)
	
	print('query (parse) :',parsed.query)
	print('query (split) :',split1.query)
	
	print('fragment (parse) :',parsed.fragment)
	print('fragment (split) :',split1.fragment)
	
	print('username (parse) :',parsed.username)
	print('username (split) :',split1.username)
	
	print('password (parse) :',parsed.password)
	print('password (split) :',split1.password)

	print('hostname (parse) :',parsed.hostname)
	print('hostname (split) :',split1.hostname)

	print('port (parse) :',parsed.port)
	print('port (split) :',split1.port)

if __name__ == "__main__":
	main()
from jinja2 import Environment, FileSystemLoader

env = Environment(loader=FileSystemLoader('templates'))

env.get_template('index.html').stream().dump('docs/index.html')
env.get_template('cards.html').stream().dump('docs/cards.html')
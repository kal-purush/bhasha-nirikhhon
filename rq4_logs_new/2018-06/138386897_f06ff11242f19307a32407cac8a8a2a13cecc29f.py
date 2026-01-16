import pandas as pd
from jinja2 import Template


# analyze
df = pd.read_json('./spy.json', orient='index')
df['rating'] = df['positive'] / (df['positive'] + df['negative'])
df['score'] = (df.median_forever * df.average_forever) ** 0.5 * 100 / 60 / df.price * df.rating
pdf = df[
    (df.positive + df.negative >= 500) 
    & (df.price > 0)].sort_values('score', ascending=False)

# write
with open('template.html') as ifs:
    template_str = ifs.read()

template = Template(template_str)
with open('index.html', 'wb') as ofs:
    ofs.write(template.render(df=pdf).encode('utf-8'))
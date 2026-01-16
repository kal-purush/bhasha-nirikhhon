from setuptools import setup

setup(name='stwitto',
      version='0.1',
      description='Command Line Twitter Client',
      url='https://github.com/surajnarwade/stwitto.git',
      author='Suraj Narwade',
      author_email='surajnarwade353@gmail.com',
      license='MIT',
      packages=['stwitto'],
      install_requires=[
          'tweepy',
      ],
      entry_points={'console_scripts': ['stwitto=stwitto.twitt:main']},
      data_files=[('/etc/stwitto/', ['config.ini'])]
      )
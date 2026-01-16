
USER = {
    'test': '',
    'prod': ''
}

PASSWORD = {
    'test': '',
    'prod': ''
}

PATH_PROJECT = {
    'test': '',
    'prod': ''
}

PATH_PROJECT_SRC = {
    'test': PATH_PROJECT['test'] + '/src',
    'prod': PATH_PROJECT['prod'] + '/src'
}

PATH_VENV = {
    'test': PATH_PROJECT['test'] + '/venv',
    'prod': PATH_PROJECT['prod'] + '/venv'
}

PATH_VENV_ACTIVATE = {
    'test': 'source  ' + PATH_VENV['test'] + '/bin/activate',
    'prod': 'source  ' + PATH_VENV['prod'] + '/bin/activate',
}
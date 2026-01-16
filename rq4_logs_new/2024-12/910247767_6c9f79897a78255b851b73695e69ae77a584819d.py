try :
    import django
    message : str = f'current version : {django.__version__}'
except ImportError as errors :
    print(error)
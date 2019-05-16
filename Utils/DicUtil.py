
def props(obj):
    '''将class转dict,以_开头的属性不要'''
    pr = {}
    for name in dir(obj):
            value = getattr(obj, name)
            if not name.startswith('__') and not callable(value) and not name.startswith('_'):
                pr[name] = value
    return pr
def props_with_(obj):
    ''' 将class转dict,以_开头的也要'''
    pr = {}
    for name in dir(obj):
        value = getattr(obj, name)
        if not name.startswith('__') and not callable(value):
            pr[name] = value
    return pr
def dict2obj(obj, dict):
    '''dict转obj，先初始化一个obj'''
    obj.__dict__.update(dict)
    return obj

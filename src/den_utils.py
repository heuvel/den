import datetime

date_patterns = ['%Y-%m-%d'     #2016-03-15 
                ,'%Y-%m-%d %H:%M:%S.0'   #2016-03-15 18:01:24.0
                ]

def is_date(s_date):
    for pattern in date_patterns:
        try:
            dt = datetime.datetime.strptime(s_date, pattern).date()
            return True
        except:
            pass
    return False

def get_date_pattern(s_date):
    for pattern in date_patterns:
        try:
            dt = datetime.datetime.strptime(s_date, pattern).date()
            return pattern
        except:
            pass
    return
 
    
def is_ts(min_val,max_val):
    result = False
    try:
        min_val_l = long(float(str(min_val)))
        max_val_l = long(float(str(max_val)))
        if min_val_l > 946684800 and max_val_l < 1577836800: #between 2000 and 2020
            result = True
    except:
        pass
    return result
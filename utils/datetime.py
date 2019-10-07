from datetime import datetime

def day_from_unix_epoch(date=datetime.today()):
    epoch = datetime.utcfromtimestamp(0)
    d = date - epoch
    return d.days
from math import radians, cos, sin, asin, sqrt


def get_distance(lng1, lat1, lng2, lat2):
    lon1, lat1, lon2, lat2 = map(radians, [lng1, lat1, lng2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * asin(sqrt(a))
    r = 6371.393
    return c * r * 1000


def get_timespan(stamp1, stamp2):
    return stamp2 - stamp1


def log(file, message):
    print(message)
    file.write(message + '\n')

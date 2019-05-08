import time
from multiprocessing import Pool
from velocity import calc_routes_velocity

if __name__ == '__main__':
    date = '2016-11-12'
    file_name_list = set([
        'didi_points_r0t100',
        'didi_points_r100t200',
        'didi_points_r200t300',
        'didi_points_r300t400',
        'didi_points_r400t500',
        'didi_points_r500t700'
    ])
    timeStamp = int(time.mktime(time.strptime('{} 00:00:00'.format(date), "%Y-%m-%d %H:%M:%S")))
    pool = Pool(processes=len(file_name_list))
    for file_name in file_name_list:
        print('{} process begin'.format(file_name))
        p = pool.apply_async(
            func=calc_routes_velocity,
            args=(file_name, timeStamp),
            callback=lambda _: print('{} process end'.format(file_name))
        )
    pool.close()
    pool.join()

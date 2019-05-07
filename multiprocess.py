from multiprocessing import Pool
from velocity import calc_routes_velocity

if __name__ == '__main__':
    file_name_list = [
        'didi_points_r0t100',
        'didi_points_r100t200',
        'didi_points_r200t300',
        'didi_points_r300t400',
        'didi_points_r400t500',
        'didi_points_r500t700',
    ]
    pool = Pool(processes=len(file_name_list))
    for file_name in file_name_list:
        print('{} 进程开始'.format(file_name))
        p = pool.apply_async(func=calc_routes_velocity, args=(file_name,), callback=lambda _: print('{} 进程结束'.format(file_name)))
    pool.close()
    pool.join()

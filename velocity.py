import sys
import time
import csv
import numpy as np
from osgeo import ogr
from utils import get_distance, get_timespan, log


def calc_routes_velocity(file_name):

    if file_name is None:
        sys.exit('文件名不能为空')

    points_matched_ds = ogr.Open('input/{}.shp'.format(file_name), 0)
    log_file = open('output/log/{}.log'.format(file_name), 'w', newline='')

    if points_matched_ds is None or log_file is None:
        sys.exit('无法打开文件')

    points_matched_layer = points_matched_ds.GetLayer(0)
    points_matched_layer_name = points_matched_layer.GetName()

    # car_id_col = 'Field1'
    order_id_col = 'Field2'
    timestamp_col = 'Field3'
    lng_col = 'Field4'
    lat_col = 'Field5'
    # target_fid_col = 'TARGET_FID'
    join_fid_col = 'JOIN_FID'

    time_start_all = time.time()
    time_start_module = time.time()

    log(log_file, 'begin {} ({})'.format(points_matched_layer_name, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))

    route_dict = {}

    for point in points_matched_layer:
        route_id = point.GetField(join_fid_col)
        order_id = point.GetField(order_id_col)
        point_info = {
            # 'route_id': route_id,
            # 'order_id': order_id,
            # 'car_id': point.GetField(car_id_col),
            'timestamp': point.GetField(timestamp_col),
            'lng': point.GetField(lng_col),
            'lat': point.GetField(lat_col),
        }
        if route_id in route_dict:
            if order_id in route_dict[route_id]:
                route_dict[route_id][order_id].append(point_info)
            else:
                route_dict[route_id][order_id] = [point_info, ]
        else:
            route_dict[route_id] = {order_id: [point_info, ]}

    time_end_module = time.time()
    log(log_file, 'step1: {:.2f}s'.format(time_end_module - time_start_module))
    time_start_module = time_end_module

    loop_count = 0
    routes_velocity_list = []
    for route_key in route_dict:
        order_dict = route_dict[route_key]
        order_mean_velocity_list = []
        for order_key in order_dict:
            point_list = order_dict[order_key]
            point_count = len(point_list)
            order_velocity_list = []
            if point_count > 1:
                point_list.sort(key=lambda x: x['timestamp'])
                for point_index in range(1, point_count):
                    distance = get_distance(
                        point_list[point_index - 1]['lng'],
                        point_list[point_index - 1]['lat'],
                        point_list[point_index]['lng'],
                        point_list[point_index]['lat']
                    )
                    timespan = get_timespan(
                        point_list[point_index - 1]['timestamp'],
                        point_list[point_index]['timestamp']
                    )
                    if timespan > 0:
                        order_velocity_list.append(distance / timespan)
            if len(order_velocity_list) != 0:
                order_mean_velocity_list.append(np.mean(order_velocity_list))
        if len(order_mean_velocity_list) != 0:
            routes_velocity_list.append({
                'route_id': route_key,
                'velocity': np.mean(order_mean_velocity_list)
            })
        time_end_module = time.time()
        loop_count += 1
        log(log_file, 'step2 {}/{}: {:.2f}s'.format(loop_count, len(route_dict), time_end_module - time_start_module))
        time_start_module = time_end_module

    time_end_module = time.time()
    log(log_file, 'step1: {:.2f}s'.format(time_end_module - time_start_module))
    time_start_module = time_end_module

    del points_matched_ds

    with open('output/{}.csv'.format(file_name), 'w', newline='') as output_file:
        routes_velocity_list.sort(key=lambda x: x['route_id'])
        headers = ['route_id', 'velocity']
        csv_writer = csv.DictWriter(output_file, headers)
        csv_writer.writerows(routes_velocity_list)

    time_end_all = time.time()
    log(log_file, 'step3: {:.2f}s'.format(time_end_all - time_start_module))
    log(log_file, 'end: {:.2f}s'.format(time_end_all - time_start_all))
    log(log_file, 'end {} ({})'.format(points_matched_layer_name, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))

    log_file.close()

import csv

import get_missing_rides


# sample data about Beit-Shemesh bus-lines
kav_470_from_Jm_to_Bs = [7005, 3]
kav_470_from_Bs_to_Jm = [7005, 3]

kav_615_from_Jm_to_Bs = [20309, 34]
kav_615_from_Bs_to_Jm = [20310, 34]

kav_616_from_Jm_to_Bs = [20301, 34]
kav_616_from_Bs_to_Jm = [20302, 34]

kav_617_from_Jm_to_Bs = [32541, 34]
kav_617_from_Bs_to_Jm = [32542, 34]

kav_618_from_Jm_to_Bs = [32544, 34]
kav_618_from_Bs_to_Jm = [32545, 34]

kav_619_from_Jm_to_Bs = [32547, 34]
kav_619_from_Bs_to_Jm = [32548, 34]

kav_620_from_Jm_to_Bs = [20305, 34]
kav_620_from_Bs_to_Jm = [20306, 34]


bs_kavim = {
    '470': [kav_470_from_Jm_to_Bs, kav_470_from_Bs_to_Jm],
    '615': [kav_615_from_Jm_to_Bs, kav_615_from_Bs_to_Jm],
    '616': [kav_616_from_Jm_to_Bs, kav_616_from_Bs_to_Jm],
    '617': [kav_617_from_Jm_to_Bs, kav_617_from_Bs_to_Jm],
    '618': [kav_618_from_Jm_to_Bs, kav_618_from_Bs_to_Jm],
    '619': [kav_619_from_Jm_to_Bs, kav_619_from_Bs_to_Jm],
    '620': [kav_620_from_Jm_to_Bs, kav_620_from_Bs_to_Jm]
}

def get_line_operator_ref(kav_short_name, kav_direction):
    if kav_short_name in bs_kavim:
        # assuming kav_direction is 0 or 1 !!
        line_ref_and_operator_ref_of_kav = bs_kavim[kav_short_name][kav_direction]

    return line_ref_and_operator_ref_of_kav[0], line_ref_and_operator_ref_of_kav[1]

def main1():
    debug : bool = False
    get_missing_rides.find_gtfs_and_siri_for_dates("2022-12-", "615", 0, 22, 22, debug)
    # find_gtfs_and_siri_for_dates("2022-12-", "470", 1, 12, 13, debug)

def find_gtfs_and_siri_for_sample_kavim(debug: bool = False):
    day_from = 1
    day_to = 26
    DIRECTION_0 = 0
    DIRECTION_1 = 1
    with open("/tmp/statistics.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(get_missing_rides.RideStatisticsOutput.csv_format())
        for kav_num in range(615, 621):
            direction_0_line_ref, direction_0_operator_ref = get_line_operator_ref(str(kav_num), DIRECTION_0)
            stats = get_missing_rides.find_gtfs_and_siri_for_dates("2022-12-", direction_0_operator_ref, direction_0_line_ref, day_from, day_to, debug)
            for s in stats:
                writer.writerow(s.to_csv_line())
            direction_1_line_ref, direction_1_operator_ref = get_line_operator_ref(str(kav_num), DIRECTION_1)
            stats = get_missing_rides.find_gtfs_and_siri_for_dates("2022-12-", direction_1_operator_ref, direction_1_line_ref, day_from, day_to, debug)
            for s in stats:
                writer.writerow(s.to_csv_line())

def main():
    debug : bool = False
    find_gtfs_and_siri_for_sample_kavim(debug)


if __name__ == '__main__':
    main()
    # s = '2022-12-11T04:00:00+02:00'
    # d = dt.strptime(s, "%Y-%m-%dT%H:%M:%S%z")
    # print(d.astimezone(tz.utc))
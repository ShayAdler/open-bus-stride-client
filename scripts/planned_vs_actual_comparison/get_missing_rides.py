# Python script to find GTFS+SIRI data about some specific bus-lines
import csv
import datetime
import time
from datetime import datetime as dt
from open_bus_stride_api.routers.gtfs_stops import GtfsStopPydanticModel
import stride
from stride.common import parse_date_str
import collections

# data about Beit-Shemesh bus-lines
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

class GtfsRoute:
    def __init__(self, short_name: str, operator: int, long_name: str, direction: int):
        self.short_name = short_name
        self.long_name = long_name
        self.direction = direction
        self.operator = operator

    @classmethod
    def from_dict(cls, dct):
        return GtfsRoute(
            short_name=dct["route_short_name"], operator=dct["operator_ref"],
            long_name=dct["route_long_name"], direction=dct["route_direction"])


class RideDetails:
    def __init__(self, start_time, journey_ref):
        self.start_time = start_time
        self.journey_ref = journey_ref


class Ride:
    def __init__(self, gtfs_time=None, siri_time=None):
        self.siri_time = siri_time
        self.gtfs_time = gtfs_time

    def __lt__(self, other):
        return (self.gtfs_time or self.siri_time) < (other.gtfs_time or other.siri_time)


class RideStatisticsOutput:
    def __init__(self, route: GtfsRoute, date: datetime.datetime, rides_planned_count: int, rides_executed_count: int, rides: [Ride]):
        self.route = route
        self.executed_rides_count = rides_executed_count
        self.planned_rides_count = rides_planned_count
        self.rides = rides
        self.date = date
        self.missing_planned_rides_count = get_missing_rides(self.rides)
        self.missing_percentage = int((self.missing_planned_rides_count / self.planned_rides_count) * 100)

    def to_dict(self):
        dict = self.__dict__
        self.rides.sort()
        dict["rides"] = [child.__dict__ for child in dict["rides"]]
        return dict

    @classmethod
    def csv_format(cls):
        return "route_name", "long_name", "operator", "direction", "full_date", "month", "day_of_the_week", "planned_rides_count", "executed_rides_count", "missing_planned_rides_count", "missing_percentage"

    # Use to create a csv line
    def to_csv_line(self):
        # route_name, long_name, operator, direction, full_date, month, day_of_the_week, planned_rides_count, executed_rides_count, missing_planned_rides_count
        return self.route.short_name, self.route.long_name, self.route.operator, self.route.direction, \
               self.date, self.date.month, self.date.strftime("%A"), \
               self.planned_rides_count, self.executed_rides_count, self.missing_planned_rides_count, self.missing_percentage

    def __str__(self):
        output = f'Planned and actual rides of {self.route.short_name} at {self.date}\n'
        self.rides.sort()
        output += "\n".join([f'{ride.gtfs_time or "     "}\t{ride.siri_time or ""}' for ride in self.rides])
        output += f'\n{self.route.short_name}-{self.route.direction} at {self.date.strftime("%A")} {self.date} missing rides: {self.planned_rides_count - self.executed_rides_count} (of {self.planned_rides_count} )'
        return output


def convert_siri_journey_ref_to_gtfs_format(siri_journey_ref: str) -> str:
    split_journey_ref = siri_journey_ref.split("-")
    return "{}_{}{}{}".format(split_journey_ref[3], split_journey_ref[2], split_journey_ref[1], split_journey_ref[0][2:])

def find_GTFS_rides_of_kav_between_kav(specified_date, hour_from, hour_to, kav_short_name, kav_direction, debug: bool = False):
    if kav_short_name in bs_kavim:
        # assuming kav_direction is 0 or 1 !!
        line_ref_and_operator_ref_of_kav = bs_kavim[kav_short_name][kav_direction]
    else:
        if debug:
            print('Kav', kav_short_name, 'details not found, skipping!')
        return []
    gtfs_rides = stride.get('/gtfs_rides/list', {
                                    'gtfs_route__date_from':specified_date,
                                    'gtfs_route__date_to':specified_date,
                                    'gtfs_route__line_refs':line_ref_and_operator_ref_of_kav[0],
                                    'gtfs_route__operator_refs': line_ref_and_operator_ref_of_kav[1],
                                    'order_by': 'start_time asc'
                                    })
    if debug:
        for x in gtfs_rides:
            print(israel_time_from_date_time(x['start_time']))
    return gtfs_rides

def find_GTFS_rides_of_kav_between(specified_date, hour_from, hour_to, operator_ref, line_ref, debug: bool = False):
    gtfs_rides = stride.get('/gtfs_rides/list', {
                                    'gtfs_route__date_from':specified_date,
                                    'gtfs_route__date_to':specified_date,
                                    'gtfs_route__line_refs':line_ref,
                                    'gtfs_route__operator_refs': operator_ref,
                                    'order_by': 'start_time asc'
                                    })
    if debug:
        for x in gtfs_rides:
            print(israel_time_from_date_time(x['start_time']))
    return gtfs_rides


def find_SIRI_rides_of_kav_between_kav(specified_date, hour_from, hour_to, kav_short_name, kav_direction, debug: bool = False):
    if kav_short_name in bs_kavim:
        # assuming kav_direction is 0 or 1 !!
        line_ref_and_operator_ref_of_kav = bs_kavim[kav_short_name][kav_direction]
    if debug:
        print('prepare for siri_rides/list with line_refs=', line_ref_and_operator_ref_of_kav[0], ', operator_refs=', line_ref_and_operator_ref_of_615[1])
    hour_from_2_digits = hour_from.rjust(2, '0')
    hour_to_2_digits = hour_to.rjust(2, '0')
    hour_arg_from = 'T' + hour_from_2_digits + ":00:00+02:00"
    hour_arg_to = 'T' + hour_to_2_digits + ":00:00+02:00"
    if int(hour_to_2_digits)>=24:
        hour_arg_to = 'T' + "23:59:00+02:00"
    siri_route = stride.get('/siri_routes/list',
                            {'line_refs': line_ref_and_operator_ref_of_kav[0],
                            'operator_refs': line_ref_and_operator_ref_of_kav[1]})
    if len(siri_route) != 1:
        print(f'got unexpected number of siri routes. operator_ref: {line_ref_and_operator_ref_of_kav[1]}, line_ref: {line_ref_and_operator_ref_of_kav[0]}, routes count: {len(siri_route)}')

    siri_rides = stride.get('/siri_rides/list', {
        'siri_route_ids': siri_route[0]["id"],
        # the following 2 arguments narrow the results to rides between hour_from and hour_to (GMT+2)
        'scheduled_start_time_from': specified_date + hour_arg_from,  # format "2022-12-11" + "T07:00:00+02:00"
        'scheduled_start_time_to': specified_date + hour_arg_to,
        'order_by': 'scheduled_start_time'})
    if debug:
        print('/siri_rides/list returns', len(siri_rides), 'records:')
    for x in siri_rides:
        # x is a dictionary like {'id': 25960774, 'siri_route_id': 7969, 'journey_ref': '2022-12-11-584795736', 'scheduled_start_time': '2022-12-11T04:00:00+00:00', 'vehicle_ref': '39273201',...}
        if debug:
            print(x['scheduled_start_time'])
    return siri_rides

def find_SIRI_rides_of_kav_between(specified_date, hour_from, hour_to, operator_ref, line_ref, debug: bool = False):
    hour_from_2_digits = hour_from.rjust(2, '0')
    hour_to_2_digits = hour_to.rjust(2, '0')
    hour_arg_from = 'T' + hour_from_2_digits + ":00:00+02:00"
    hour_arg_to = 'T' + hour_to_2_digits + ":00:00+02:00"
    if int(hour_to_2_digits)>=24:
        hour_arg_to = 'T' + "23:59:00+02:00"
    siri_route = stride.get('/siri_routes/list',
                            {'line_refs': line_ref,
                            'operator_refs': operator_ref})
    if len(siri_route) != 1:
        print(f'got unexpected number of siri routes. operator_ref: {operator_ref}, line_ref: {line_ref}, routes count: {len(siri_route)}')

    siri_rides = stride.get('/siri_rides/list', {
        'siri_route_ids': siri_route[0]["id"],
        # the following 2 arguments narrow the results to rides between hour_from and hour_to (GMT+2)
        'scheduled_start_time_from': specified_date + hour_arg_from,  # format "2022-12-11" + "T07:00:00+02:00"
        'scheduled_start_time_to': specified_date + hour_arg_to,
        'order_by': 'scheduled_start_time'})
    if debug:
        print('/siri_rides/list returns', len(siri_rides), 'records:')
    for x in siri_rides:
        # x is a dictionary like {'id': 25960774, 'siri_route_id': 7969, 'journey_ref': '2022-12-11-584795736', 'scheduled_start_time': '2022-12-11T04:00:00+00:00', 'vehicle_ref': '39273201',...}
        if debug:
            print(x['scheduled_start_time'])
    return siri_rides


def time_from_date_time(ddt: str):
    # dt is like 2022-12-23T08:00:00+00:00
    s = ddt.split('T')
    tt = s[1]
    ss = tt.split("+")
    th = ss[0]
    return th


def weekday_from_date(the_date):
    d = dt.strptime(the_date, "%Y-%m-%d")     # d is datetime object
    # return d.weekday()
    return d.strftime("%A")


def israel_time_from_date_time(ddt: str):
    # ddt is like '2022-12-11T08:00:00+00:00'
    d = ddt
    # astimezone() changes to GMT+2 because this is the locale of the system (machine on which this code runs)
    dd = d.replace(microsecond=0).astimezone().isoformat()      # 2022-12-23T10:00:00+02:00
    #print(dd)
    time_only_in_ISR_clock = d.astimezone().time().strftime("%H:%M")
    return time_only_in_ISR_clock


def compare_daily_schedules_actual_and_planned(gtfs_times: list[RideDetails], siri_times: list[RideDetails]) -> list[Ride]:
    siri_rides_count = collections.defaultdict(int)
    for ride in siri_times:
        siri_rides_count[ride.start_time] += 1
    gtfs_rides_count = collections.defaultdict(int)
    for ride in gtfs_times:
        gtfs_rides_count[ride.start_time] += 1

    rides = []
    for gtfs_time, planned_rides_count in gtfs_rides_count.items():
        actual_rides_count = siri_rides_count.get(gtfs_time, 0)
        if actual_rides_count == planned_rides_count:
            rides.extend([Ride(gtfs_time, gtfs_time) for _ in range(planned_rides_count)])
        elif planned_rides_count > actual_rides_count:
            rides.extend([Ride(gtfs_time, gtfs_time) for _ in range(actual_rides_count)])
            missing_rides = planned_rides_count - actual_rides_count
            rides.extend([Ride(gtfs_time, None) for _ in range(missing_rides)])
        else:
            rides.extend([Ride(gtfs_time, gtfs_time) for _ in range(planned_rides_count)])
            extra_rides = actual_rides_count - planned_rides_count
            rides.extend([Ride(None, gtfs_time) for _ in range(extra_rides)])

    return rides

# Comparing only against the planned rides, to ignore cases in which we have
# more actual rides for the same planned ride.
def get_missing_rides(daily_rides: list[Ride]) -> int:
    return len([ride for ride in daily_rides if ride.gtfs_time is not None and ride.siri_time is None])


def before_4(x: str):
    # x is '00:00' or 01:00'
    h = int(x.split(':')[0])
    if h < 4:
        return True
    else:
        return False


def remove_previous_night(gtfs_times: list[RideDetails]):
    ret = [x for x in gtfs_times if not before_4(x.start_time)]
    return ret


def find_gtfs_and_siri_for_dates_kav(year_month: str, kav, kav_direction, day_from, day_to, debug: bool = False):
    for day in range(int(day_from), 1+int(day_to)):
        the_date = year_month + str(day).rjust(2, '0')
        find_gtfs_and_siri_for_date(the_date, kav, kav_direction, debug)


def find_gtfs_and_siri_for_dates(year_month: str, operator_ref, line_ref, day_from, day_to, debug: bool = False) -> list[RideStatisticsOutput]:
    output = []
    for day in range(int(day_from), 1+int(day_to)):
        the_date = year_month + str(day).rjust(2, '0')
        statistics = find_gtfs_and_siri_for_date(the_date, operator_ref, line_ref, debug)
        if statistics is not None: # should only happen on Saturdays
            output.append(statistics)

    return output

def find_gtfs_and_siri_for_date(the_date, operator_ref, line_ref, debug: bool = False) -> RideStatisticsOutput:
    # kav = "617"
    # kav_direction = 1   # 0 or 1!!
    h_from = '4'
    h_to = '25'
    gtfs_rides = find_GTFS_rides_of_kav_between(the_date, h_from, h_to, operator_ref, line_ref, debug)
    if len(gtfs_rides) == 0:
        # currently skip kavim for which I have no GTFS details about
        return



    res = stride.get('/gtfs_routes/list', {
        'date_from': the_date,
        'date_to': the_date,
        'line_refs': line_ref,
        'operator_refs': operator_ref,
        # 'order_by': 'start_time asc'
    })
    route = GtfsRoute.from_dict(res[0])


    gtfs_times = [RideDetails(start_time=israel_time_from_date_time(x['start_time']), journey_ref=x['journey_ref']) for x in gtfs_rides]
    gtfs_times = remove_previous_night(gtfs_times)
    siri_rides = find_SIRI_rides_of_kav_between(the_date, h_from, h_to, operator_ref, line_ref, debug)
    if debug:
        print('/siri_rides/list returns', len(siri_rides), 'records:')
    start_times_with_tz = [RideDetails(start_time=israel_time_from_date_time(x['scheduled_start_time']), journey_ref=convert_siri_journey_ref_to_gtfs_format(x['journey_ref'])) for x in siri_rides]
    # for x in start_times_gmt_0:
    #     print(x)
    # TODO: add kav metadata -
    # Line
    # Direction
    # Operator
    # Line
    # origin
    # Line
    # direction
    # Date
    # Day in the
    # week
    compared_rides = compare_daily_schedules_actual_and_planned(gtfs_times, start_times_with_tz)
    statistics = RideStatisticsOutput(route, parse_date_str(the_date), len(gtfs_times), len(start_times_with_tz),
                                      compared_rides)
    print(statistics)
    return statistics


def find_gtfs_and_siri_for_all_kavim(debug: bool = False):
    day_from = 1
    day_to = 26
    DIRECTION_0 = 0
    DIRECTION_1 = 1
    for kav_num in range(615, 621):
        find_gtfs_and_siri_for_dates("2022-12-", str(kav_num), DIRECTION_0, day_from, day_to, debug)
        find_gtfs_and_siri_for_dates("2022-12-", str(kav_num), DIRECTION_1, day_from, day_to, debug)


def main1():
    debug : bool = False
    find_gtfs_and_siri_for_dates("2022-12-", "615", 0, 22, 22, debug)
    # find_gtfs_and_siri_for_dates("2022-12-", "470", 1, 12, 13, debug)


def main():
    debug : bool = False
    find_gtfs_and_siri_for_all_kavim(debug)


if __name__ == '__main__':
    main()
    # s = '2022-12-11T04:00:00+02:00'
    # d = dt.strptime(s, "%Y-%m-%dT%H:%M:%S%z")
    # print(d.astimezone(tz.utc))

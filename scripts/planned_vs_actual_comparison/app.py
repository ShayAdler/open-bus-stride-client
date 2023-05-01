import csv

from flask import Flask, Response, make_response
from get_missing_rides import find_gtfs_and_siri_for_date, parse_date_str, RideStatisticsOutput
from io import StringIO
from datetime import timedelta


app = Flask(__name__)


@app.route('/')
def hello():
    return 'Hello, World!'


@app.route('/get_rides_statistics/<line_ref>/<operator_ref>/<date>')
def get_rides_statistics(line_ref=None, operator_ref=None, date=None):
    response = find_gtfs_and_siri_for_date(date, operator_ref, line_ref)
    if response is None:
        return {}

    response = make_response(response.to_dict())
    response.headers.add("Access-Control-Allow-Origin", "*")
    return response


@app.route('/get_statistics_csv/<line_refs>/<operator_refs>/<date_from>/<date_to>')
def get_statistics_csv(line_refs=None, operator_refs=None, date_from=None, date_to=None):
    line_refs = line_refs.split(",")
    operator_refs = operator_refs.split(",")
    if len(line_refs) != len(operator_refs):
        return Response("line_refs and operator_refs should be corresponding", status=400, mimetype='application/json')

    csv_data = StringIO()
    writer = csv.writer(csv_data)
    writer.writerow(RideStatisticsOutput.csv_format())
    delta = parse_date_str(date_to) - parse_date_str(date_from)
    for operator_ref, line_ref in zip(line_refs, operator_refs):
        for i in range(delta.days + 1):
            day = parse_date_str(date_from) + timedelta(days=i)
            date_statistics = find_gtfs_and_siri_for_date(day.__str__(), line_ref, operator_ref)
            if date_statistics is not None:
                writer.writerow(date_statistics.to_csv_line())

    csv_data.seek(0)
    response = make_response(csv_data)
    response.headers["Content-Disposition"] = f"attachment; filename={'_'.join(line_refs)}-{date_from}-{date_to}-statistics.csv"
    response.headers["Content-type"] = "text/csv"
    return response


if __name__ == '__main__':
    app.run("0.0.0.0", 5001)



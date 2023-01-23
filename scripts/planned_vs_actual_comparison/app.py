from flask import Flask
from get_missing_rides import find_gtfs_and_siri_for_date

app = Flask(__name__)


@app.route('/')
def hello():
    return 'Hello, World!'

@app.route('/get_rides_statistics/<line_ref>/<operator_ref>/<date>')
def get_rides_statistics(line_ref=None, operator_ref=None, date=None):
    return find_gtfs_and_siri_for_date(date, operator_ref, line_ref).to_dict()

if __name__ == '__main__':
    app.run()



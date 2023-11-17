from flask import Flask, request, jsonify, Response
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta, timezone
import pandas as pd
import schedule
import threading
import time
import uuid

app = Flask(__name__)

# Define the SQLAlchemy database models
Base = declarative_base()

class Activity(Base):
    __tablename__ = 'activity'
    id = Column(Integer, primary_key=True)
    store_id = Column(String)
    timestamp_utc = Column(DateTime)
    status = Column(String)

class BusinessHours(Base):
    __tablename__ = 'business_hours'
    id = Column(Integer, primary_key=True)
    store_id = Column(String)
    day_of_week = Column(Integer)
    start_time_local = Column(String)
    end_time_local = Column(String)

class Timezone(Base):
    __tablename__ = 'timezone'
    store_id = Column(String, primary_key=True)
    timezone_str = Column(String)

class ReportStatus(Base):
    __tablename__ = 'report_status'
    id = Column(String, primary_key=True)
    status = Column(String, default='Running')

# Connect to the database
engine = create_engine('sqlite:///restaurant_monitoring.db', echo=True)
Base.metadata.create_all(bind=engine)

# Create a session to interact with the database
Session = sessionmaker(bind=engine)
session = Session()

# Helper function to get the local time from UTC time
def convert_utc_to_local(utc_time, store_timezone):
    utc_time = utc_time.replace(tzinfo=timezone.utc)
    local_timezone = timezone(store_timezone)
    local_time = utc_time.astimezone(local_timezone)
    return local_time

# Fetch data from CSVs and update the database
def update_database():
    try:
        activity_df = pd.read_csv("C:\\Users\\abhig\\Downloads\\store status.csv")
        business_hours_df = pd.read_csv("C:\\Users\\abhig\\Downloads\\Menu hours.csv")
        timezone_df = pd.read_csv("C:\\Users\\abhig\\Downloads\\bq-results-20230125-202210-1674678181880.csv")

        # Replace the existing data in the database with the new data
        activity_df.to_sql('activity', con=engine, if_exists='replace', index=False)
        business_hours_df.to_sql('business_hours', con=engine, if_exists='replace', index=False)
        timezone_df.to_sql('timezone', con=engine, if_exists='replace', index=False)

    except Exception as e:
        print(f"Error updating database: {e}")

# Schedule data update every hour
schedule.every().hour.do(update_database)

# Calculate downtime and uptime metrics within business hours
def calculate_metrics(store_id, business_hours, start_time, end_time):
    # Fetch activity data for the specified store within the given time range
    activity_data = session.query(Activity).filter(
        Activity.store_id == store_id,
        Activity.timestamp_utc >= start_time,
        Activity.timestamp_utc <= end_time
    ).all()

    downtime_minutes = 0
    uptime_minutes = 0

    business_hours_start = datetime.strptime(business_hours['start_time_local'], '%H:%M:%S').time()
    business_hours_end = datetime.strptime(business_hours['end_time_local'], '%H:%M:%S').time()

    last_observation_time = None

    for activity in activity_data:
        local_time = convert_utc_to_local(activity.timestamp_utc, business_hours['timezone_str'])
        local_time_time = local_time.time()

        if business_hours_start <= local_time_time <= business_hours_end:
            if last_observation_time is not None:
                time_diff = (local_time - last_observation_time).total_seconds() / 60.0

                if activity.status == 'inactive':
                    downtime_minutes += time_diff
                else:
                    uptime_minutes += time_diff

            last_observation_time = local_time

    return downtime_minutes, uptime_minutes

# Extrapolate metrics for the entire business hours interval
def extrapolate_metrics(business_hours, last_observation_time, now_utc):
    business_hours_start = datetime.strptime(business_hours['start_time_local'], '%H:%M:%S').time()
    business_hours_end = datetime.strptime(business_hours['end_time_local'], '%H:%M:%S').time()

    total_business_hours = (business_hours_end.hour - business_hours_start.hour) * 60 + (business_hours_end.minute - business_hours_start.minute)

    if last_observation_time is None:
        return 0, total_business_hours

    time_diff = (now_utc - last_observation_time).total_seconds() / 60.0

    return time_diff, total_business_hours - time_diff

# API endpoint to trigger report generation
@app.route('/trigger_report', methods=['GET'])
def trigger_report():
    report_id = str(uuid.uuid4())  # Generate a random report_id

    # Insert report status into the database
    new_report_status = ReportStatus(id=report_id)
    session.add(new_report_status)
    session.commit()

    # Trigger report generation
    threading.Thread(target=generate_report, args=(report_id,)).start()

    return jsonify({'report_id': report_id})

# API endpoint to get report status or the CSV
@app.route('/get_report', methods=['GET'])
def get_report():
    report_id = request.args.get('report_id')

    # Fetch report status from the database
    report_status = session.query(ReportStatus).filter(ReportStatus.id == report_id).first()

    if not report_status:
        return jsonify({'error': 'Report not found.'}), 404

    if report_status.status == 'Complete':
        # Fetch and return the CSV file
        report_csv = session.query(Activity).filter(Activity.store_id == report_id).to_df().to_csv(index=False)
        response = Response(report_csv, mimetype='text/csv')
        response.headers["Content-Disposition"] = f"attachment; filename={report_id}_report.csv"
        return response
    elif report_status.status == 'Running':
        return jsonify({'status': 'Running'})
    else:
        return jsonify({'status': 'Error'})

# Function to generate the report in the background
def generate_report(report_id):
    now_utc = datetime.utcnow()

    # Fetch business hours for the store
    business_hours = session.query(BusinessHours).filter(BusinessHours.store_id == report_id, BusinessHours.day_of_week == now_utc.weekday()).first()

    if business_hours:
        business_hours_start = datetime.strptime(business_hours.start_time_local, '%H:%M:%S')
        business_hours_end = datetime.strptime(business_hours.end_time_local, '%H:%M:%S')

        # Calculate metrics for the last hour within business hours
        last_hour_start = max(now_utc - timedelta(hours=1), business_hours_start)
        downtime_last_hour, uptime_last_hour = calculate_metrics(report_id, business_hours, last_hour_start, now_utc)

        # Calculate metrics for the last day within business hours
        last_day_start = max(now_utc - timedelta(days=1), business_hours_start)
        downtime_last_day, uptime_last_day = calculate_metrics(report_id, business_hours, last_day_start, now_utc)

        # Calculate metrics for the last week within business hours
        last_week_start = max(now_utc - timedelta(weeks=1), business_hours_start)
        downtime_last_week, uptime_last_week = calculate_metrics(report_id, business_hours, last_week_start, now_utc)

        # Extrapolate metrics for the entire business hours interval
        last_observation_time = session.query(Activity.timestamp_utc).filter(Activity.store_id == report_id).order_by(Activity.timestamp_utc.desc()).first()
        last_observation_time = last_observation_time[0] if last_observation_time else None

        extrapolate_downtime_last_hour, extrapolate_uptime_last_hour = extrapolate_metrics(business_hours, last_observation_time, now_utc)

        # Update report status in the database
        report_status = session.query(ReportStatus).filter(ReportStatus.id == report_id).first()
        report_status.status = 'Complete'
        session.commit()
    else:
        # Update report status in the database
        report_status = session.query(ReportStatus).filter(ReportStatus.id == report_id).first()
        report_status.status = 'Error'
        session.commit()

def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    # Start the scheduler thread
    scheduler_thread = threading.Thread(target=run_scheduler)
    scheduler_thread.start()

    app.run(debug=False)

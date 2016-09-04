import os
import csv
import simplejson as simplejson
from db import DB
from flask import Blueprint, request
from flask_cors import CORS
from sqlalchemy import func

from authentication import Authentication
from schema import FollowingStatus, ProfilesApi, ProfilesUsers, JobDetails

api_twitter_follow = Blueprint('api_twitter_follow', __name__)
CORS(api_twitter_follow)


class Follow:
    @api_twitter_follow.route("/follow", methods=['POST'])
    def followUser(self):
        try:
            req_json = request.get_json()
        except Exception, e:
            print e

        profile_api = req_json['profile_api']
        profile_user = req_json['profile_user']

        endpoint = "https://api.twitter.com/1.1/friendships/create.json?screen_name=" + profile_user + "&follow=true"
        api_handler = Authentication().twitter(profile_api)
        res, data = api_handler.request(endpoint, "POST")
        return data

    @api_twitter_follow.route("/health", methods=['GET'])
    def health(self):
        return 'health_status: OK'


@api_twitter_follow.route('/follow/bulk/csv', methods=['POST'])
def followBulkCsv():
    file = request.files['file']
    job_name = request.form['job_name']
    print job_name
    print "success"
    try:
        path_dir= os.path.dirname(os.path.abspath(__file__))+"\csv"
        file_path = os.path.join(path_dir, file.filename)
        print file_path
        file.save(file_path)
        entries = 0
        in_process = 0
        completed = 0
        ob_db = DB()
        session = ob_db.session()
        with open(file_path, 'rb') as f:
            next(f)
            reader = csv.reader(f)
            for row in reader:
                print(row)

                try:
                    profile_api_name = row[0]
                except IndexError:
                    profile_api_name = ''

                try:
                    profile_user_name = row[1]
                except IndexError:
                    return "#PROFILE USER COLUMN CANNOT BE EMPTY"

                if profile_api_name is '':
                    print ("no data")
                else:
                    rows = session.query(ProfilesApi).filter_by(twitter_handle=profile_api_name).first()
                    print("3")
                    if rows is None:
                        return "#NO PROFILE API FOUND FOR USER:" + str(profile_api_name)

                print("2")
        with open(file_path, 'rb') as f:
            next(f)
            reader = csv.reader(f)
            for row in reader:
                print row
                print("2111")
                try:
                    profile_api_name = row[0]
                except IndexError:
                    profile_api_name = ''


                profile_user_name = row[1]

                if profile_api_name is '':
                    profile_api_id = None
                else:
                    rows = session.query(ProfilesApi).filter_by(twitter_handle=profile_api_name).first()
                    profile_api_id = rows.id

                rows = session.query(ProfilesUsers).filter_by(name=profile_user_name).first()

                if rows is None:
                    new_data = ProfilesUsers(name=profile_user_name)
                    session.add(new_data)
                    rows = session.query(ProfilesUsers).filter_by(name=profile_user_name).first()
                    profile_user_id = rows.id
                else:
                    profile_user_id = rows.id

                '''
                if profile_api_name is None:
                    rows = session.query(func.min(ProfilesApi.ts_update)).first()
                    profile_api_id = rows.id
                    profile_api_name=rows.twitter_handle

                '''
                new_data = FollowingStatus(profile_api_id, profile_user_id, 'PENDING', job_name)
                session.add(new_data)

                '''
                endpoint = "https://api.twitter.com/1.1/friendships/create.json?screen_name=" + profile_user_name + "&follow=true"
                api_handler = Authentication().twitter(profile_api_name)
                res, data = api_handler.request(endpoint, "POST")
                print data
                new_data = FollowingStatus(profile_api_id, profile_user_id, 'FOLLOWING', job_name)
                session = ob_db.session()
                session.add(new_data)
                session.commit()
                '''
                entries += 1
        new_data = JobDetails(status='INPROCESS', entries=entries, failed=0, completed=0, job_name=job_name)
        session.add(new_data)
        session.commit()
        try:
            os.remove(file_path)
        except Exception as ex:
            print(ex)
        all_followers = session.query(JobDetails).all()
        followingstatus_as_dict = []
        for followingstatus in all_followers:
            dt_obj = followingstatus.ts_create
            date_str_cr = dt_obj.strftime("%Y-%m-%d")

            followingstatu_as_dict = {

                'id': followingstatus.id,
                'status': followingstatus.status,
                'jobName': followingstatus.job_name,
                'entries': followingstatus.entries,
                'failed': followingstatus.failed,
                'completed': followingstatus.completed,
                'created_on': date_str_cr,
                'updated_on': '-'}
            print(followingstatus_as_dict)
            followingstatus_as_dict.append(followingstatu_as_dict)

        print simplejson.dumps(followingstatus_as_dict)
        return simplejson.dumps(followingstatus_as_dict)
    except Exception as ex:
        print (ex)
        return "#FILE_NOT_SAVED"


@api_twitter_follow.route('/follow/bulk', methods=['POST'])
def followBulk():
    try:
        ob_db = DB()
        session = ob_db.session()
        all_followers = session.query(JobDetails).all()
        followingstatus_as_dict = []
        for followingstatus in all_followers:
            dt_obj = followingstatus.ts_create
            date_str_cr = dt_obj.strftime("%Y-%m-%d")

            followingstatu_as_dict = {

                'id': followingstatus.id,
                'status': followingstatus.status,
                'jobName': followingstatus.job_name,
                'entries': followingstatus.entries,
                'failed': followingstatus.failed,
                'completed': followingstatus.completed,
                'created_on': date_str_cr,
                'updated_on': '-'}
            print(followingstatus_as_dict)
            followingstatus_as_dict.append(followingstatu_as_dict)

        print simplejson.dumps(followingstatus_as_dict)
        return simplejson.dumps(followingstatus_as_dict)
    except Exception as ex:
        print (ex)
        return "NODATA"
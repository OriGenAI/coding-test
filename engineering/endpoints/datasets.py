import json
import os

from endpoints.library import Library
from flask_restful import Resource, reqparse
from flask import request
from endpoints.utils import get_job_info
import requests


class Datasets(Resource):
    def __init__(self):
        self.library = Library()

    def get(self):
        """
        Gets all datasets that exist in the whole database.
        #TODO implement ways to scope this down to a list of dataset IDs
        Required params:
          - N/A
        Return:
          - a 'Result' containing a list of all datasets within the entire client's account
          - 200 code
        """
        # check for token validation
        response, code = self.library.validate_token(request)
        if code != 200:
            return response, code

        username = response.get('Result').get('email')
        db_name = response.get('Result').get('custom:client')
        parser = reqparse.RequestParser()
        parser.add_argument(
            'type', required=False)
        args = parser.parse_args()
        
        if args['type'] == 'raw':
            return self.raw_dataset_helper(db_name)
        elif args['type'] == 'processed':
            return self.processed_dataset_helper(db_name)
        else:
            return self.all_datasets_helper(db_name)

    def raw_dataset_helper(self, db_name):
        """
        Limits the scope of the query to raw datasets only
        """
        conn = self.library.mysql_authenticate()
        statement = "SELECT data_id, raw_id, dstruct_id, data_name, config, loc, t_create, t_modify, meta FROM {}.datasets WHERE raw_id = -1".format(
            db_name)

        result = self.library.make_query(conn, statement, 0)
        # conn.close()
        clean_result = self.library.clean_json(result)
        return {'Result': clean_result}, 200

    def processed_dataset_helper(self, db_name):
        """
        Limits the scope of the query to processed datasets only
        """
        conn = self.library.mysql_authenticate()
        statement = "SELECT data_id, raw_id, dstruct_id, data_name, config, loc, t_create, t_modify, meta FROM {}.datasets WHERE raw_id != -1".format(
            db_name)

        result = self.library.make_query(conn, statement, 2)

        # conn.close()
        clean_result = self.library.clean_json(result)
        return {'Result': clean_result}, 200
    
    def all_datasets_helper(self, db_name):
        """
        Queries all datasets
        """
        conn = self.library.mysql_authenticate()
        statement = "SELECT data_id, raw_id, dstruct_id, config, data_name, dstruct_id, loc, t_create, t_modify, meta FROM {}.datasets".format(db_name)

        result = self.library.make_query(conn, statement, 0)
        for dataset in result:
            job_info = get_job_info(
                library=self.library, conn=conn, schema=db_name, d_id=dataset['data_id'])
            if len(job_info) > 0:
                dataset['status'] = job_info['status']
                dataset['progress'] = job_info['progress']
                dataset['eta'] = job_info['eta']

        # conn.close()
        clean_result = self.library.clean_json(result)
        return {'Result': clean_result}, 200

    def post(self):
        """
        Inserts a new dataset into the DB
        Requried params:
          - type: 'raw' or 'processed'
          - data_name: the name of the new dataset
        IF type == 'raw':
          - sim_list: a string containing every simulation zip file that will makeup this dataset. This is used to 
            create the presigned urls from the generic data storage service
        IF type == 'processed':
          - dstruct_id: the id of the dstruct for this dataset
          - config: all the info needed to process the dataset
          - raw_id: the id of the raw dataset from which the new processed one will be created
        Return:
          - a 'Result' containing a mapping of each given simulation zip file to an upload object
          - 200 code
        """
        # check for token validation
        response, code = self.library.validate_token(request)
        if code != 200:
            return response, code

        username = response.get('Result').get('email')
        db_name = response.get('Result').get('custom:client')

        required_params = ['data_name', 'type']
        parser = reqparse.RequestParser()

        for p in required_params:
            parser.add_argument(
                p, required=True, help='{} can not be blank!'.format(p))
        args = parser.parse_args()
        conn = self.library.mysql_authenticate()
        full_response = None
        if args['type'] == 'raw':
            full_response = self.handle_raw_post(
                conn=conn, db_name=db_name, args=args, parser=parser)
        elif args['type'] == 'process':
            full_response = self.handle_process_post(
                conn=conn, db_name=db_name, args=args, parser=parser)
        else:
            full_response = {
                'Result': '\'type\' should be \'raw\' or \'process\'!'}, 400

        return full_response

    def handle_raw_post(self, conn, db_name, args, parser):
        """
        Manages the flow for positng a new raw dataset with sim_cases. Communicates with GDSS
        to get pre-signed URLs
        """
        print('raw')
        parser.add_argument('sim_list', required=True,
                            help='sim_list can not be blank!')
        args = parser.parse_args()
        # put everything into the same dataset directory in S3, with the name being this dataset's ID,
        # so that this loc points at a single directory
        statement = "INSERT into {}.datasets (data_name, loc, dstruct_id) VALUES (\'{}\', \'{}\', {})".format(
            db_name, args['data_name'], 'not_assigned', 0)
        self.library.make_query(conn, statement, 2)
        # conn.commit()

        # get data_id that was just created, we need this for the S3 key
        statement = "SELECT LAST_INSERT_ID();"
        data_id = self.library.make_query(conn, statement, 3)['id']

        # folder in S3 in datasets is named after the data_id
        dir_name = os.path.join(str(db_name), 'datasets')
        dir_name = os.path.join(dir_name, 'raw')
        dir_name = os.path.join(dir_name, '{}'.format(data_id))

        # update the loc field for this dataset entry
        statement = "UPDATE {}.datasets set loc = \'{}\' WHERE data_id = {}".format(
            db_name, dir_name, data_id)
        self.library.make_query(conn, statement, 2)
        # conn.commit()
        # conn.close()

        # get an upload link for every simulation file
        sim_list = args['sim_list'].replace(' ', '').split(',')

        full_response = []
        for sim in sim_list:
            # TODO move this into a library function
            params = {'key': os.path.join(dir_name, sim)}
            response = self.library.make_query(conn, params, 4)
            if response['status_code'] != 200:
                return response['Result'], response['status_code']
            upload_url_map = {'Item': sim, 'Response': response['Result']}
            full_response.append(upload_url_map)
        return {'Result': full_response}, 201

    def handle_process_post(self, conn, db_name, args, parser):
        """
        Manages flow for posting a new processed dataset. Communicates with the batch job system
        for dataset processing.
        """
        print('processed')
        required_params = ['dstruct_id', 'config', 'raw_id']
        for p in required_params:
            parser.add_argument(
                p, required=True, help='{} can not be blank!'.format(p))
        args = parser.parse_args()
        # setup config dictionary
        args['config'] = json.loads(args['config'])
        args['config']['schema'] = db_name

        # create a new dataset, get bucket and key information and put into config object
        statement = "INSERT into {}.datasets (data_name, loc, dstruct_id, raw_id) VALUES (\'{}\', \'{}\', {}, {})".format(
            db_name, args['data_name'], 'not_assigned', args['dstruct_id'], args['raw_id'])
        self.library.make_query(conn, statement, 2)
        # conn.commit()


        # get data_id that was just created, we need this for the S3 key
        statement = "SELECT LAST_INSERT_ID();"
        data_id = self.library.make_query(conn, statement, 5)['id']

        # folder in S3 in datasets is named after the data_id
        processed_key = os.path.join(str(db_name), 'datasets')
        processed_key = os.path.join(processed_key, 'processed')
        processed_key = os.path.join(
            processed_key, '{}.zip'.format(str(data_id)))

        statement = "SELECT loc FROM {}.datasets WHERE data_id = {}".format(
            db_name, args['raw_id'])
        raw_key = self.library.make_query(conn, statement, 6)['loc']
        args['config']['raw_bucket'] = self.library.config['bucket']
        args['config']['raw_key'] = raw_key
        args['config']['processed_bucket'] = self.library.config['bucket']
        args['config']['processed_key'] = processed_key
        args['config']['dstruct_id'] = args['dstruct_id']
        
        # finally update the processed dataset with this information
        new_config = {
            'x_radius': args['config']['x_radius'], 'y_radius': args['config']['y_radius']}
        reformatted_config = json.dumps(new_config)
        statement = "UPDATE {}.datasets set loc = \'{}\', config = \'{}\' WHERE data_id = {}".format(
            db_name, processed_key, reformatted_config, data_id)
        self.library.make_query(conn, statement, 2)
        # conn.commit()
        statement = "INSERT into {}.data_process_jobs (data_id) VALUES ({})".format(
            db_name, data_id)
        self.library.make_query(conn, statement, 2)
        # conn.commit()
        
        # get the dpj_id from this insert
        statement = "SELECT LAST_INSERT_ID();"
        dpj_id = self.library.make_query(conn, statement, 5)['id']
        args['config']['dpj_id'] = dpj_id

        # submit job for dataset processing
        job_submit_response = self.library.submit_job(
            name=args['data_name'].replace(' ', '_'), params=args['config'])
        statement = "UPDATE {}.data_process_jobs set batch_job_id = \'{}\' where dpj_id = {}".format(
            db_name, job_submit_response['job_id'], dpj_id)
        self.library.make_query(conn, statement, 2)
        # conn.commit()
        # conn.close()
        return {'Result': {'data_id': data_id, 'job_id': job_submit_response['job_id'], 'dpj_id': dpj_id}}, 201


class Dataset(Resource):
    def __init__(self):
        self.library = Library()

    def get(self, id):
        """
        Gets the dataset information designated by the given dataset id
        Required params:
          - N/A
        Return:
          - a 'Result' with the desired dataset information
          - 200 code
        """
        # check for token validation
        response, code = self.library.validate_token(request)
        if code != 200:
            return response, code

        username = response.get('Result').get('email')
        db_name = response.get('Result').get('custom:client')

        conn = self.library.mysql_authenticate()

        # check for user rights to add a dataset to this project
        statement = "SELECT data_id, data_name, dstruct_id, config, loc, t_create, t_modify, meta FROM {}.datasets WHERE data_id = {}".format(
            db_name, id)
        result = self.library.make_query(conn, statement, 7)
        if len(result) == 0:
            return {'Result': 'The requested dataset is not found within this user\'s scope.'}, 404
        job_info = get_job_info(
            library=self.library, conn=conn, schema=db_name, d_id=result['data_id'])
        if len(job_info) > 0:
            result['status'] = job_info['status']
            result['progress'] = job_info['progress']
            result['eta'] = job_info['eta']
        clean_result = self.library.clean_json(result)
        return {'Result': clean_result}, 200

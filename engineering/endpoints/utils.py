import json
import os


def get_job_info(library, conn, schema, d_id):
    statement = 'SELECT * FROM {}.data_process_jobs WHERE data_id = {}'.format(schema, d_id)
    response = library.make_query(conn, statement, 1)

    if len(response) > 0:
        return response
    else:
        return []
    
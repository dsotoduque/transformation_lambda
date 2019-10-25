from __future__ import print_function
from neo4j.v1 import GraphDatabase, basic_auth
import json
import base64

print('Loading function')


def lambda_handler(event, context):
    output = []

    for record in event['records']:
        print(record['recordId'])
        payload = base64.b64decode(record['data'])

        payload = transform_data(payload)
        encode_str_base = json.dumps(payload)
        encoded_bytes = base64.b64encode(encode_str_base.encode("utf-8"))
        encoded_str = str(encoded_bytes, "utf-8")

        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': encoded_str
        }
        output.append(output_record)
        print(output)

    print('Successfully processed {} records.'.format(len(event['records'])))

    return {'records': output}


def transform_data(data):
    node = mapper(data)
    reduce_outcome = reducer(node)
    return reduce_outcome

def mapper(data):
    data = json.loads(data)
    print(data)
    nodeStruct={
        'id': data['_id'],
        'projectCode': data['projectCode']
    }
    print(nodeStruct)
    return nodeStruct
    
def reducer(node):
    print(node["id"])
    neo4j_url = 'bolt://db-xvwxuy811zirtlf14lmz.graphenedb.com:24787'
    driver = GraphDatabase.driver(neo4j_url, auth=basic_auth('dbopsuser', 'b.G6oUmNKKs9MD.3Gxn1WfHSoUw0OGa'))
    session = driver.session()
    session.run("MERGE(n:Project {id: $idProject, projectCode: $projectCode}) RETURN n",
                idProject=node["id"], projectCode=node["projectCode"])
    session.close()
    print(node)
    return node
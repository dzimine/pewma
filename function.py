import math
import json
import copy

'''
Probabalistic Exponentially Weighted Moving Average PEWMA algorithm as described in
Carter, Kevin M., and William W. Streilein. "Probabilistic reasoning for streaming anomaly detection." Statistical Signal Processing Workshop (SSP), 2012 IEEE. IEEE, 2012.

The original code is from https://aws.amazon.com/blogs/iot/anomaly-detection-using-aws-iot-and-aws-lambda/
As Greengrass lambda can be long-running, the time-series points are stored in memory
between invocations (no need for DynamoDB).
'''


class Table(object):
    '''
    In-memory storage with DynamoDB like interface to preserve resablance with the cloud version
    of this lambda from https://aws.amazon.com/blogs/iot/anomaly-detection-using-aws-iot-and-aws-lambda/
    '''

    def __init__(self, keyname):
        self._db = {}
        self._keyname = keyname

    def get_item(self, Key):
        key = Key[self._keyname]
        print key
        return self._db.get(key)

    def put_item(self, Item):
        key = Item[self._keyname]
        self._db[key] = Item

# TODO: read from env variables
T = 30
alpha_0 = 0.95
beta = 0.5
threshold = .05
data_cols = ["Wind_Velocity_Mtr_Sec"]
key_param = "Station_Name"

table = Table(key_param)


def lambda_handler(event, context):
    '''
    the parameters than need to be changed for a different data set are T, alpha_0, beta, data_cols and key_param
    -T: number of points to consider in initial average
    -alpha_0: base amount of PEWMA which is made up from previous value of PEWMA
    -beta: parameter that controls how much you allow outliers to affect your MA, for standard EWMA set to 0
    -threshold: value below which a point is considered an anomaly, like a probability but not strictly a probability
    -data_cols: columns we want to take PEWMA of
    -key_param: key from event which will become dynamo key
    '''
    print "------event"
    print event
    response = table.get_item(Key={key_param: event[key_param]})  # get record from dynamodb for this sensor
    if response:
        newRecord = response
        newRecord = update_list_of_last_n_points(event, newRecord, data_cols, T)
        newRecord = generate_pewma(newRecord, event, data_cols, T, alpha_0, beta, threshold)
    else:
        newRecord = initial_dynamo_record(event, data_cols)
        print "writing initial dynamo record"
        print newRecord
    # create record to republish to IoT- all decimals must be changed back to floats
    iot_repub(newRecord, data_cols)
    table.put_item(Item=newRecord)  # write new record to dynamo
    return{"it": "worked"}


def update_list_of_last_n_points(event, current_data, data_cols, length_limit):
    '''
    this function updates lists that contain length_limit # of most recent points
    '''
    new_data = current_data
    for col in event:
        if col in data_cols:
            append_list = current_data[col]
            append_list.append(event[col])
            if len(append_list) > length_limit:
                append_list = append_list[1:]
            new_data[col] = append_list
        else:
            new_data[col] = event[col]
    return new_data


def initial_dynamo_record(event, data_cols):
    '''
    if there is no record in dynamodb for this sensorid then this will generate
    the record which will be the initial record
    '''
    newRecord = copy.deepcopy(event)
    for col in event:
        if col in data_cols:
            newRecord[col] = [newRecord[col]]
            newRecord["alpha_" + col] = 0
            newRecord["s1_" + col] = event[col]
            newRecord["s2_" + col] = math.pow(event[col], 2)
            newRecord["s1_next_" + col] = newRecord["s1_" + col]
            newRecord["STD_next_" + col] = \
                math.sqrt(newRecord["s2_" + col] - math.pow(newRecord["s1_" + col], 2))
        else:
            newRecord[col] = newRecord[col]
    return newRecord


def generate_pewma(newRecord, event, data_cols, T, alpha_0, beta, threshold):
    for col in data_cols:
        t = len(newRecord[col])
        newRecord["s1_" + col] = newRecord["s1_next_" + col]
        newRecord["STD_" + col] = newRecord["STD_next_" + col]
        try:
            newRecord["Z_" + col] = (event[col] - newRecord["s1_" + col]) / newRecord["STD_" + col]
        except ZeroDivisionError:
            newRecord["Z_" + col] = 0

        newRecord["P_" + col] = \
            1 / math.sqrt(2 * math.pi) * math.exp(-math.pow(newRecord["Z_" + col], 2) / 2)
        newRecord["alpha_" + col] = \
            calc_alpha(newRecord, t, T, col, beta, alpha_0)
        newRecord["s1_" + col] = \
            newRecord["alpha_" + col] * newRecord["s1_" + col] + (1 - newRecord["alpha_" + col]) * event[col]
        newRecord["s2_" + col] = \
            newRecord["alpha_" + col] * newRecord["s2_" + col] + (1 - newRecord["alpha_" + col]) * math.pow(event[col], 2)
        newRecord["s1_next_" + col] = newRecord["s1_" + col]
        newRecord["STD_next_" + col] = \
            math.sqrt(newRecord["s2_" + col] - math.pow(newRecord["s1_" + col], 2))
        isAnomaly = newRecord["P_" + col] <= threshold
        newRecord[col + "_is_Anomaly"] = isAnomaly
    return newRecord


def calc_alpha(newRecord, t, T, col, beta, alpha_0):
    if t < T:
        alpha = 1 - 1.0 / t
        print "EWMA calc in progress (initialization of MA) -" + col
    else:
        alpha = (1 - beta * newRecord["P_" + col]) * alpha_0
        print "EWMA calc in progress-" + col
    return alpha


def iot_repub(newRecord, data_cols):
    iot_record = copy.deepcopy(newRecord)
    for col in data_cols:
        iot_record[col] = iot_record[col][-1]
    payload = json.dumps(iot_record, indent=4)
    print payload
    return

if __name__ == '__main__':
    # open JSON file
    # feed events to Lambda in the loop

    event = {
        "Ambient_Temperature_Deg_C": 17.2,
        "Global_Horizontal_Irradiance": 784.5,
        "Interval_End_Time": 1457264700000,
        "Interval_Minutes": 5,
        "Location_Label": "Muni Woods",
        "Station_ID": "SF36",
        "Station_Name": "Muni Woods",
        "Time": "11:45:00",
        "Wind_Direction_Deg": 99.6,
        "Wind_Direction_Variance_Deg": 0,
        "Wind_Velocity_Mtr_Sec": 1.353
    }

    with open('SF36_subset.json') as f:
        data = json.load(f)
    for event in data:
        lambda_handler(event, None)

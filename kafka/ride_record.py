from typing import List, Dict
from datetime import datetime

class RideRecord:
    def __init__(self, arr: List[str]):
        self.vendor_id = int(arr[0])
        self.tpep_pickup_datetime = int(datetime.strptime(arr[1], "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
        self.tpep_dropoff_datetime = int(datetime.strptime(arr[2], "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
        self.passenger_count = int(arr[3])
        self.trip_distance = float(arr[4])
        self.rate_code_id = int(arr[5])
        self.store_and_fwd_flag = arr[6]
        self.pu_location_id = int(arr[7])
        self.do_location_id = int(arr[8])
        self.payment_type = int(arr[9])
        self.fare_amount = float(arr[10])
        self.extra = float(arr[11])
        self.mta_tax = float(arr[12])
        self.tip_amount = float(arr[13])
        self.tolls_amount = float(arr[14])
        self.improvement_surcharge = float(arr[15])
        self.total_amount = float(arr[16])
        self.congestion_surcharge = float(arr[17])
    
    @classmethod
    def from_dict(cls, d: Dict):
        return cls(arr=[
            d['vendor_id'],
            d['tpep_pickup_datetime'][0],
            d['tpep_dropoff_datetime'][0],
            d['passenger_count'],
            d['trip_distance'],
            d['rate_code_id'],
            d['store_and_fwd_flag'],
            d['pu_location_id'],
            d['do_location_id'],
            d['payment_type'],
            d['fare_amount'],
            d['extra'],
            d['mta_tax'],
            d['tip_amount'],
            d['tolls_amount'],
            d['improvement_surcharge'],
            d['total_amount'],
            d['congestion_surcharge'],
        ])
    
    def __repr__(self):
        return f'{self.__class__.__name__}: {self.__dict__}'
    

def dict_to_ride_record(obj, ctx):
    if obj is None:
        return None
    
    return RideRecord.from_dict(obj)

def ride_record_to_dict(ride_record: RideRecord, ctx):
    return ride_record.__dict__
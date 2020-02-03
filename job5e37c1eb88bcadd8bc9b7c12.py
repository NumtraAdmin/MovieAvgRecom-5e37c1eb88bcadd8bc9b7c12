import traceback
import sys
from operations import TopOperation
from operations import JoinOperation
from operations import AggregationOperation
from operations import FormulaOperation
from operations import FilterOperation
from connectors import DBFSConnector
from connectors import CosmosDBConnector
from datatransformations import TranformationsMainFlow
from automl import tpot_execution
from core import PipelineNotification
import json

try: 
	PipelineNotification.PipelineNotification().started_notification('5e37c1eb88bcadd8bc9b7c13','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
	MovieAvgRecom_DBFS = DBFSConnector.DBFSConnector.fetch([], {}, "5e37c1eb88bcadd8bc9b7c13", spark, "{'url': '/Demo/Marketing/MovieRatings (2).csv', 'file_type': 'Delimeted', 'dbfs_token': 'dapi743e2d3cc92a32916f8c2fa9bd7d0606', 'dbfs_domain': 'westus.azuredatabricks.net', 'delimiter': ',', 'is_header': 'Use Header Line'}")

	PipelineNotification.PipelineNotification().completed_notification('5e37c1eb88bcadd8bc9b7c13','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e37c1eb88bcadd8bc9b7c13','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify','http://104.40.91.74:3200/logs/getProductLogs')
	sys.exit(1)
try: 
	PipelineNotification.PipelineNotification().started_notification('5e37c1eb88bcadd8bc9b7c14','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
	MovieAvgRecom_AutoFE = TranformationsMainFlow.TramformationMain.run(["5e37c1eb88bcadd8bc9b7c13"],{"5e37c1eb88bcadd8bc9b7c13": MovieAvgRecom_DBFS}, "5e37c1eb88bcadd8bc9b7c14", spark,json.dumps( {"FE": [{"transformationsData": {}, "feature": "UserId", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "25000", "mean": "462.83", "stddev": "267.09", "min": "1", "max": "943", "missing": "0"}}, {"transformationsData": {}, "feature": "MovieId", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "25000", "mean": "425.34", "stddev": "330.71", "min": "1", "max": "1682", "missing": "0"}}, {"transformationsData": {}, "feature": "Rating", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "25000", "mean": "3.53", "stddev": "1.12", "min": "1.0", "max": "5.0", "missing": "0"}, "transformation": ""}, {"transformationsData": {"feature_label": "Timestamp"}, "feature": "Timestamp", "type": "date", "selected": "True", "replaceby": "random", "stats": {"count": "", "mean": "", "stddev": "", "min": "", "max": "", "missing": "0"}, "transformation": "Extract Date"}, {"transformationsData": {}, "feature": "AvgRating", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "25000", "mean": "3.53", "stddev": "0.46", "min": "1.51", "max": "4.94", "missing": "0"}, "transformation": ""}, {"feature": "Timestamp_dayofmonth", "transformation": "", "transformationsData": {}, "type": "numeric", "selected": "True", "stats": {"count": "1000", "mean": "16.01", "stddev": "8.37", "min": "1", "max": "31", "missing": "0"}}, {"feature": "Timestamp_month", "transformation": "", "transformationsData": {}, "type": "numeric", "selected": "True", "stats": {"count": "1000", "mean": "6.94", "stddev": "4.4", "min": "1", "max": "12", "missing": "0"}}, {"feature": "Timestamp_year", "transformation": "", "transformationsData": {}, "type": "numeric", "selected": "True", "stats": {"count": "1000", "mean": "1997.45", "stddev": "0.5", "min": "1997", "max": "1998", "missing": "0"}}]}))

	PipelineNotification.PipelineNotification().completed_notification('5e37c1eb88bcadd8bc9b7c14','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e37c1eb88bcadd8bc9b7c14','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify','http://104.40.91.74:3200/logs/getProductLogs')
	sys.exit(1)
try: 
	PipelineNotification.PipelineNotification().started_notification('5e37c1eb88bcadd8bc9b7c15','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
	MovieAvgRecom_AutoML = tpot_execution.Tpot_execution.run(["5e37c1eb88bcadd8bc9b7c14"],{"5e37c1eb88bcadd8bc9b7c14": MovieAvgRecom_AutoFE}, "5e37c1eb88bcadd8bc9b7c15", spark,json.dumps( {"model_type": "regression", "label": "AvgRating", "features": ["UserId", "MovieId", "Rating", "Timestamp", "Timestamp_dayofmonth", "Timestamp_month", "Timestamp_year"], "percentage": "10", "executionTime": "5", "sampling": "0", "sampling_value": "", "run_id": "", "ProjectName": "Retail Scenarios", "PipelineName": "MovieAvgRecom", "userid": "567a95c8ca676c1d07d5e3e7", "runid": "", "url_ResultView": "http://104.40.91.74:3200", "experiment_id": "895518857185768"}))

	PipelineNotification.PipelineNotification().completed_notification('5e37c1eb88bcadd8bc9b7c15','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e37c1eb88bcadd8bc9b7c15','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify','http://104.40.91.74:3200/logs/getProductLogs')
	sys.exit(1)


import json
import requests


BIGNUMBER = 1e6
KWSLISTS=('mothers day','fathers day','superbowl','iphone 6s','graduation','world series tickets')



class ElasticSearchFactory(object):
	'''
	params ={ 'kw':'', 'starttime':'', 'endtime':'',... } 
	'''
	def __init__(self, params):
		super(ElasticSearchFactory, self).__init__()
		self.aggr_params = '?pretty&filter_path=responses._shards.failed,responses.aggregations.timeseries.buckets.key_as_string,responses.aggregations.timeseries.buckets.sum_quantity.value'
		self.us_url = 'http://10.65.195.138:9200/dw_ck_daily_*_*/kw/_search?pretty=true&size='+str(BIGNUMBER)
		self.local_url='http://10.64.200.113:9200/dw_ck_mthly_*_*/kw/_search?pretty=true&size='+str(BIGNUMBER)
		self.params = params


	def getQueryParameters(self):
		return {
			"query": {
			    "bool": {
			      "must": [
			        {
			          "range": {
			            "ck_dt": {
			              "gte": self.params['starttime'],
			              "lte": self.params['endtime'],
			              "format": "yyyy-MM-dd"
			            }
			          }
			        },
			        {
			          "match": {
			            "auct_title": {
			              "query": params['kw'],
			              "minimum_should_match": "100%"
			            }
			          }
			        }
			      ]
			    }
		  	}
		}

	def saveResults(self):
		for kw in KWSLISTS:
			w = open(kw+'_topscore.txt','w')
			data = self.getQueryParameters(self.params)
			r = requests.post( self.us_url, data=json.dumps(data) )
			jsonData = r.json()
			q_str=''
			for idx, line in enumerate(jsonData['hits']['hits']):
				q_str += str(idx+1)+' : '+str(line['_score'])+' , '+line['_source']['auct_title'].encode('utf-8')+' , '+str(line['_source']['ck_dt'])+'\n'
				
			w.write(q_str)
			w.close()


if __name__ == '__main__':
	# pass
	for kw in KWSLISTS:
		params={'kw':kw, 'starttime':'2012-08-01','endtime':'2015-07-31'}
		print json.dumps(ElasticSearchFactory(params).getQueryParameters())
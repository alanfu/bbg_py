# -*- coding: utf-8 -*-
#####################################
###### @author: alanfu        #######
###### created on May/18/2016 #######
#####################################

import blpapi
from optparse import optionParser
import datetime as dt
from collections import OrderedDict
import numpy as np
import pandas as pd

BBG_REF_DATA_SERVICE = '//blp/refdata'
BBG_REF_DATA_REQUEST = 'ReferenceDataRequest'
BBG_HISTORICAL_DATA_REQUEST = 'HistoricalDataRequest'
BBG_REQUEST_PERIODICITY = 'periodicitySelection'
BBG_REQUEST_SECURITY = 'securities'
BBG_REQUEST_FIELD = 'fields'
BBG_REQUEST_START_DT = 'startDate'
BBG_REQUEST_END_DT = 'endDate'
BBG_SECURITY_DATA = 'securityData'
BBG_FIELD_DATA = 'fieldData'
DATE_FORMAT = '%Y%m%d'

class BBGDataMgr():

	def __init__(self):
		self._options = self._parseCmdLine()
		self._start_session()
		self._initial_request()

	def _parseCmdLine(self):
		parser = optionParser(description = "Retrieve reference data")
		parser.add_option("-a",
						  "--ip",
						  dest = "host",
						  help = "server name or IP (default: %default)",
						  metavar = "ipAddress",
						  default = "localhost")
		parser.add_option("-p",
						  dest = "port",
						  help = "server port (default: %default)",
						  metavar = "tcpPort",
						  default = 8194)
		(options, args) = parser.parse_args()

		return options

	def _start_session(self):
		self._sessionOptions = blpapi.SessionOptions()
		self._sessionOptions.setServerHost(self._options.host)
		self._sessionOptions.setServerPort(self._options.port)
		self._session = blpapi.Session(self._sessionOptions)

	def _initial_request(self):
		self._session.start()
		self._session.openService(BBG_REF_DATA_SERVICE)
		self._refdataservice = self._session.getService(BBG_REF_DATA_SERVICE)

		# request for bdh
		self._ref = self._refdataservice.createRequest(BBG_HISTORICAL_DATA_REQUEST)
		self._ref.set(BBG_REQUEST_PERIODICITY, 'DAILY')

		# request for bdp
		self._ref_ref = self._refdataservice.createRequest(BBG_REF_DATA_REQUEST)

	def bdp(self, sec, field):
		###################################
		### @sec: str
		### @field: str
		###################################

		# append securities to request
		self._ref_ref.append(BBG_REQUEST_SECURITY, sec)

		# append fields to request
		self._ref_ref.append(BBG_REQUEST_FIELD, field)

		self._session.sendRequest(self._ref_ref)

		loop = True

		try:
			while loop:
				event = self._session.nextEvent()
				for msg in event:

					if event.eventType() = blpapi.Event.RESPONSE or \
					event.eventType() = blpapi.Event.PARTIAL_RESPONSE:
						securityData = msg.getElement(BBG_SECURITY_DATA).getValue()
						fieldData = securityData.getElement(BBG_FIELD_DATA)
						element = fieldData.getElement(0)
						ret = element.getValue()

						return ret

					if event.eventType() == blpapi.Event.RESPONSE:
						loop = False
		except Exception:
			raise

	def bdh(self, sec, field, *date):
		###################################
		### @sec: str
		### @field: str
		### @date: tuple(datetime.datetime)
		###################################

		# append securities to request
		# make sure only one security on request
		if self._req.getElement(BBG_REQUEST_SECURITY).numValues() > 0:
			self._req.getElement(BBG_REQUEST_SECURITY).setValue(sec)
		else:
			self._ref.getElement(BBG_REQUEST_SECURITY).appendValue(sec)

		# append fields to request
		# make sure only one field on request
		if self._req.getElement(BBG_REQUEST_FIELD).numValues() > 0:
			self._req.getElement(BBG_REQUEST_FIELD).setValue(field)
		else:
			self._ref.getElement(BBG_REQUEST_FIELD).appendValue(field)

		if len(date) == 1:
			start_dt = date[0]
		elif len(date) == 2:
			start_dt = date[0]
			end_dt = date[1]
		else:
			start_dt = dt.datetime.today()
			end_dt = start_dt
			if len(date) > 2:
				print('Too many date arguments!')
		
		self._req.set(BBG_REQUEST_START_DT, start_dt.strftime(DATE_FORMAT))
		self._req.set(BBG_REQUEST_END_DT, end_dt.strftime(DATE_FORMAT))

		self._session.sendRequest(self._ref)

		loop = True
		errorMsg = str()
		try:
			while loop:
				event = self._session.nextEvent()
				for msg in event:

					if event.eventType() = blpapi.Event.RESPONSE or \
					event.eventType() = blpapi.Event.PARTIAL_RESPONSE:
						securityData = msg.getElement(BBG_SECURITY_DATA)
						
						# check for invalid security
						if securityData.hasElement('securityError'):
							securityError = securityData.getElement('securityError')
							errorMsg = securityError.getElementAsString('message')
							raise ValueError(errorMsg)
						# check for invalid field
						else:
							fieldExceptions = securityData.getElement('fieldExceptions')
							if fieldExceptions.numValues() > 0:
								missingField = fieldExceptions.getValueAsElement(0)
								field_id = missingField.getElementAsString('fieldId')
								error_info = missingField.getElement('errorInfo')
								error_msg = error_info.getElementAsString('message')
								error_msg = error_msg + ": " + field_id
								raise ValueError(errorMsg)
						
						name = securityData.getElementAsString('security')		
						fieldData = securityData.getElement(BBG_FIELD_DATA)
						response = OrderedDict()
						
						for i in range(fieldData.numValues()):
							element = fieldData.getValueAsElement(i)
							for n in range(1, element.numElements()):
								date = element.getElementAsDatetime('date')
								field = element.getElement(n)
								response[date] = field.getValue()
						
						ret = pd.DateFrame.from_dict(response, orient = 'index')
						
						if len(ret) == 1:
							return ret.ix[0][0]
						elif len(ret) > 1:
							return ret
						else:
							raise ValueError('No data found!\n')
							
					if event.eventType() == blpapi.Event.RESPONSE:
						loop = False
		except ValueError as e:
			print(e，'\nsecurity: ',sec,'\nfield: ',field,'\nperiod: ',start_dt,'-',end_dt)
			return float('nan')
		except Exception:
			raise

if __name__ == '__main__':
	
	bbg = BBGDataMgr()
	ret1 = bbg.bdh('UXM6 Index', 'PX_LAST', dt.datetime.today())
	ret2 = bbg.bdh('ESM6 Index', 'PX_LAST', dt.datetime.today())

	# Expirty date of June SPX call 2000
	ret3 = bbg.bdp('X5M6C 2000 Index', 'LAST_TRADEABLE_DT')
	ret4 = bbg.bdh('SPY US 12/21/13 C178 Equity', 'IVOL_MID', dt.datetime(2013,11,13))

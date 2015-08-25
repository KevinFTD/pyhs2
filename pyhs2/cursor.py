#encoding=utf-8

from TCLIService.ttypes import TOpenSessionReq, TGetTablesReq, TFetchResultsReq,\
  TStatusCode, TGetResultSetMetadataReq, TGetColumnsReq, TType, TTypeId, \
  TExecuteStatementReq, TGetOperationStatusReq, TFetchOrientation, TCloseOperationReq, \
  TCloseSessionReq, TGetSchemasReq, TGetLogReq, TCancelOperationReq, TGetCatalogsReq, TGetInfoReq

from error import Pyhs2Exception
import threading
import re
import itertools

def get_type(typeDesc):
    for ttype in typeDesc.types:
        if ttype.primitiveEntry is not None:
            return TTypeId._VALUES_TO_NAMES[ttype.primitiveEntry.type]
        elif ttype.mapEntry is not None:
            return ttype.mapEntry
        elif ttype.unionEntry is not None:
            return ttype.unionEntry
        elif ttype.arrayEntry is not None:
            return ttype.arrayEntry
        elif ttype.structEntry is not None:
            return ttype.structEntry
        elif ttype.userDefinedTypeEntry is not None:
            return ttype.userDefinedTypeEntry

def get_col_value(col):
    # Could directly get index from schema but would need to cache the schema
    if col.stringVal:
      return _get_val(col.stringVal)
    elif col.i16Val is not None:
      return _get_val(col.i16Val)
    elif col.i32Val is not None:
      return _get_val(col.i32Val)
    elif col.i64Val is not None:
      return _get_val(col.i64Val)
    elif col.doubleVal is not None:
      return _get_val(col.doubleVal)
    elif col.boolVal is not None:
      return _get_val(col.boolVal)
    elif col.byteVal is not None:
      return _get_val(col.byteVal)
    elif col.binaryVal is not None:
      return _get_val(col.binaryVal)

def _get_val(colVal):
    colVal.values = set_nulls(colVal.values, colVal.nulls)
    colVal.nulls = '' # Clear the null values for not re-marking again the column with nulls at the next call
    return colVal.values

def mark_nulls(values, bytestring):
    mask = bytearray(bytestring)

    for n in mask:
        yield n & 0x01
        yield n & 0x02
        yield n & 0x04
        yield n & 0x08

        yield n & 0x10
        yield n & 0x20
        yield n & 0x40
        yield n & 0x80

def set_nulls(values, bytestring):
    if bytestring == '' or re.match('^(\x00)+$', bytestring): # HS2 has just \x00 or '', Impala can have \x00\x00...
      return values
    else:
      #return [None if is_null else value for value, is_null in itertools.izip(values, cls.mark_nulls(values, bytestring))]
      _values = [None if is_null else value for value, is_null in itertools.izip(values, mark_nulls(values, bytestring))]
      if len(values) != len(_values): # HS2 can have just \x00\x01 instead of \x00\x01\x00...
        _values.extend(values[len(_values):])
      return _values

class Cursor(object):
    session = None
    client = None
    operationHandle = None
    hasMoreRows = True
    MAX_BLOCK_SIZE = 10000
    arraysize = 1000
    _currentRecordNum = None
    _currentBlock = None
    _standbyBlock = None
    _blockRequestInProgress = False
    _cursorLock = None

    def __init__(self, _client, sessionHandle):
        self.session = sessionHandle
        self.client = _client
        self._cursorLock = threading.RLock()

    def execute(self, hql):
        self.hasMoreRows = True
        query = TExecuteStatementReq(self.session, statement=hql, confOverlay={})
        res = self.client.ExecuteStatement(query)
        self.operationHandle = res.operationHandle
        if res.status.errorCode is not None:
            raise Pyhs2Exception(res.status.errorCode, res.status.errorMessage)
        
    def ifetch(self):
        while self.hasMoreRows:
             for row in self._ifetchSet():
                 yield row

    def _ifetchSet(self):
        fetchReq = TFetchResultsReq(operationHandle=self.operationHandle,
                                    orientation=TFetchOrientation.FETCH_NEXT,
                                    maxRows=10000)
        return self._ifetch(fetchReq)

    def fetch(self):
        rows = list()
        while self.hasMoreRows:
             rows += self._fetchSet()
        return rows

    def _fetchSet(self):
        fetchReq = TFetchResultsReq(operationHandle=self.operationHandle,
                                    orientation=TFetchOrientation.FETCH_NEXT,
                                    maxRows=10000)
        rows = self._fetch(fetchReq)
        return rows

    def _fetchBlock(self):
        """ internal use only.
	 get a block of rows from the server and put in standby block.
         future enhancements:
         (1) locks for multithreaded access (protect from multiple calls)
         (2) allow for prefetch by use of separate thread
        """
        # make sure that another block request is not standing
        if self._blockRequestInProgress :
           # need to wait here before returning... (TODO)
           return

        # make sure another block request has not completed meanwhile
        if self._standbyBlock is not None: 
           return

        self._blockRequestInProgress = True
        fetchReq = TFetchResultsReq(operationHandle=self.operationHandle,
                                    orientation=TFetchOrientation.FETCH_NEXT,
                                    maxRows=self.arraysize)
        self._standbyBlock = self._fetch(fetchReq)
        self._blockRequestInProgress = False
        return

    def fetchone(self):
        """ fetch a single row. a lock object is used to assure that a single 
	 record will be fetched and all housekeeping done properly in a 
	 multithreaded environment.
         as getting a block is currently synchronous, this also protects 
	 against multiple block requests (but does not protect against 
	 explicit calls to to _fetchBlock())
        """
        self._cursorLock.acquire()

        # if there are available records in current block, 
	# return one and advance counter
        if self._currentBlock is not None and self._currentRecordNum < len(self._currentBlock):
           x = self._currentRecordNum
           self._currentRecordNum += 1
           self._cursorLock.release()
           return self._currentBlock[x]

        # if no standby block is waiting, fetch a block
        if self._standbyBlock is None:
           # TODO - make sure exceptions due to problems in getting the block 
	   # of records from the server are handled properly
           self._fetchBlock()

        # if we still do not have a standby block (or it is empty), 
	# return None - no more data is available
        if self._standbyBlock is None or len(self._standbyBlock)==0:
           self._cursorLock.release()
           return None

        #  move the standby to current
        self._currentBlock = self._standbyBlock 
        self._standbyBlock = None
        self._currentRecordNum = 1

        # return the first record
        self._cursorLock.release()
        return self._currentBlock[0]

    def fetchmany(self,size=-1):
        """ return a sequential set of records. This is guaranteed by locking, 
	 so that no other thread can grab a few records while a set is fetched.
         this has the side effect that other threads may have to wait for 
         an arbitrary long time for the completion of the current request.
        """
        self._cursorLock.acquire()

        # default value (or just checking that someone did not put a ridiculous size)
        if size < 0 or size > self.MAX_BLOCK_SIZE:
           size = self.arraysize
        recs = []
        for i in range(0,size):
            recs.append(self.fetchone())

        self._cursorLock.release()
        return recs

    def fetchall(self):
        """ returns the remainder of records from the query. This is 
	 guaranteed by locking, so that no other thread can grab a few records 
	 while the set is fetched. This has the side effect that other threads 
	 may have to wait for an arbitrary long time until this query is done 
	 before they can return (obviously with None).
        """
        self._cursorLock.acquire()

        recs = []
        while True:
            rec = self.fetchone()
            if rec is None:
               break
            recs.append(rec)

        self._cursorLock.release()
        return recs

    def __iter__(self):
        """ returns an iterator object. no special code needed here. """
        return self

    def next(self):
        """ iterator-protocol for fetch next row. """
        row = self.fetchone()
        if row is None:
           raise StopIteration 
        return row

    def getSchema(self):
        if self.operationHandle:
            req = TGetResultSetMetadataReq(self.operationHandle)
            res = self.client.GetResultSetMetadata(req)
            if res.schema is not None:
                cols = []
                for c in self.client.GetResultSetMetadata(req).schema.columns:
                    col = {}
                    col['type'] = get_type(c.typeDesc)
                    col['columnName'] = c.columnName
                    col['comment'] = c.comment
                    cols.append(col)
                return cols
        return None

    def getDatabases(self):
        req = TGetSchemasReq(self.session)
        res = self.client.GetSchemas(req)
        self.operationHandle = res.operationHandle
        if res.status.errorCode is not None:
            raise Pyhs2Exception(res.status.errorCode, res.status.errorMessage)
        return self.fetch()

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        self.close()

    def _fetch(self, fetchReq):
        resultsRes = self.client.FetchResults(fetchReq)

        colValList = self._getValuesFromColBasedRes(resultsRes.results)
        rows = zip(*colValList)

        return rows

    def _ifetch(self, fetchReq):
        resultsRes = self.client.FetchResults(fetchReq)

        colValList = self._getValuesFromColBasedRes(resultsRes.results)
        rows = itertools.izip(*colValList)

        return rows

    def _getValuesFromColBasedRes(self, colBasedResults):
        # 当columns为空，或者其中一列为空
        if not colBasedResults.columns or not get_col_value(colBasedResults.columns[0]):
            self.hasMoreRows = False

        columns_list = colBasedResults.columns
        if not hasattr(columns_list, '__iter__'):
            return list()

        columns_val_list = list()
        for column in columns_list:
            val_list = get_col_value(column)
            columns_val_list.append(val_list)

        return columns_val_list

    def close(self):
        if self.operationHandle is not None:
            req = TCloseOperationReq(operationHandle=self.operationHandle)
            self.client.CloseOperation(req) 

import urlparse
import httplib
from urllib import urlencode
import json
import zlib

class IdsClient(object):
    
    def __init__(self, url):
        o = urlparse.urlparse(url)
        self.secure = o.scheme == "https"
        self.ids_host = o.netloc
        path = o.path
        if not path.endswith("/"): path = path + "/"
        self.path = path + "ids/"
        
    def ping(self):
        result = self._process("ping", {}, "GET").read()
        if not result == "IdsOK": 
            raise IdsException("NotFoundException", "Server gave invalid response: " + result)
            
    def getServiceStatus(self, sessionId):
        parameters = {}
        parameters["sessionId"] = sessionId; 
        result = self._process("getServiceStatus", parameters, "GET").read()
        return json.loads(result)
    
    def getStatus(self, sessionId, datafileIds=[], datasetIds=[], investigationIds=[]):
        parameters = {}
        parameters["sessionId"] = sessionId;   
        _fillParms(parameters, datafileIds, datasetIds, investigationIds)
        return  self._process("getStatus", parameters, "GET").read()
    
    def restore(self, sessionId, datafileIds=[], datasetIds=[], investigationIds=[]):
        parameters = {"sessionId": sessionId}
        _fillParms(parameters, datafileIds, datasetIds, investigationIds)
        self._process("restore", parameters, "POST", headers={"Content-Type":"application/x-www-form-urlencoded"}).read()
        
    def archive(self, sessionId, datafileIds=[], datasetIds=[], investigationIds=[]):
        parameters = {"sessionId": sessionId}
        _fillParms(parameters, datafileIds, datasetIds, investigationIds)
        self._process("restore", parameters, "POST", headers={"Content-Type":"application/x-www-form-urlencoded"}).read()
      
    def isPrepared(self, preparedId):
        parameters = {"preparedId": preparedId}
        if self._process("isPrepared", parameters, "GET"): return True
        return False
    
    def getData(self, sessionId, datafileIds=[], datasetIds=[], investigationIds=[], compressFlag=False, zipFlag=False, outname=None, offset=0):
        parameters = {"sessionId": sessionId}
        _fillParms(parameters, datafileIds, datasetIds, investigationIds)
        if zipFlag:  parameters["zip"] = "true";
        if compressFlag: parameters["compress"] = "true";
        if outname: parameters["outname"] = outname
        if offset: headers = {"Range": "bytes=" + str(offset) + "-"}
        return self._process("getData", parameters, "GET", headers=headers)
      
    def put(self, sessionId, inputStream, name, datasetId,
            datafileFormatId, description=None, doi=None, datafileCreateTime=None,
             datafileModTime=None): 
        parameters = {"sessionId": sessionId , "name":name, "datasetId": str(datasetId), "datafileFormatId": str(datafileFormatId)}
        if description: parameters["description"] = description
        if doi: parameters["doi"] = doi
        if datafileCreateTime: parameters["datafileCreateTime"] = str(datafileCreateTime)
        if datafileModTime: parameters["datafileModTime"] = str(datafileModTime)
        if not inputStream: raise IdsException("BadRequestException", "Input stream is null")
        
        result, crc = self._process("put", parameters, "PUT", body=inputStream)
        om = json.loads(result.read())
        if om["checksum"] != crc: raise IdsException("InternalException", "Error uploading - the checksum was not as expected")
        return long(om["id"]);
      
    def _process(self, relativeUrl, parameters, method, headers=None, body=None):
        path = self.path + relativeUrl
        if parameters: parameters = urlencode(parameters)
        if parameters and method != "POST":
            path = path + "?" + parameters
        if self.secure:
            conn = httplib.HTTPSConnection(self.ids_host)
        else:
            conn = httplib.HTTPConnection(self.ids_host)
        conn.putrequest(method, path, skip_accept_encoding=True)
        conn.putheader("Cache-Control", "no-cache")
        conn.putheader("Pragma", "no-cache")
        conn.putheader("Accept", "text/html, image/gif, image/jpeg, *; q=.2, */*; q=.2")
        conn.putheader("Connection", "keep-alive") 
        
        if parameters and method == "POST":
            conn.putheader('Content-Length', str(len(parameters)))
        elif body:
            conn.putheader('Transfer-Encoding', 'chunked')
           
        if headers:
            for header in headers:
                print header, headers[header]
                conn.putheader(header, headers[header])
        conn.endheaders()
        
        if parameters and method == "POST":
            conn.send(parameters)
        elif body:
            blocksize = 8192
            datablock = body.read(blocksize)
            crc32 = 0
            while datablock:
                conn.send(hex(len(datablock))[2:] + "\r\n")
                conn.send(datablock + "\r\n")
                crc32 = zlib.crc32(datablock, crc32)
                datablock = body.read(blocksize)
            conn.send("0\r\n\r\n")
       
        response = conn.getresponse()
        rc = response.status
        if (rc / 100 != 2):
            om = json.loads(response.read())
            code = om["code"]
            message = om["message"]
            raise IdsException(code, message)
        if body:
            return response, crc32 & 0xffffffff
        else:
            return response
        
def _fillParms(parameters, dfIds, dsIds, invIds):
    if invIds:
        parameters["investigationIds"] = ",".join(str(x) for x in invIds);
    if dsIds:
        parameters["datasetIds"] = ",".join(str(x) for x in dsIds);
    if dfIds:
        parameters["datafileIds"] = ",".join(str(x) for x in dfIds);

class IdsException(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message
        
    def __str__(self):
        return self.code + ": " + self.message
     

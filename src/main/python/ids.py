import urlparse
import httplib
from urllib import urlencode
import json
import zlib
import getpass

class IdsClient(object):
    
    def __init__(self, url):
        """
        Create an IdsClient. The url should have the scheme, hostname and optionally the port. 
        It may also have a path if it is installed behind an apache front end. 
        """
        o = urlparse.urlparse(url)
        self.secure = o.scheme == "https"
        self.ids_host = o.netloc
        path = o.path
        if not path.endswith("/"): path = path + "/"
        self.path = path + "ids/"
        
    def getApiVersion(self):
        """
        Get the version of the server
        """
        return self._process("getApiVersion", {}, "GET").read()
    
    def isReadOnly(self):
        """
        See if the server is configured to be readonly
        """
        return self._process("isReadOnly", {}, "GET").read() == "true"
    
    def isTwoLevel(self):
        """
        See if the server is configured to use both main and archive storage
        """
        return self._process("isTwoLevel", {}, "GET").read() == "true"
    
    def ping(self):
        """
        Check that the server is alive and is an IDS server
        """
        result = self._process("ping", {}, "GET").read()
        if not result == "IdsOK": 
            raise IdsException("NotFoundException", "Server gave invalid response: " + result)
            
    def getServiceStatus(self, sessionId):
        """
        Return information about what the IDS is doing. If all lists are empty it is quiet.
     
        To use this call, the user represented by the sessionId must be in the set of rootUserNames
        defined in the IDS configuration.
        """
        parameters = {}
        parameters["sessionId"] = sessionId; 
        result = self._process("getServiceStatus", parameters, "GET").read()
        return json.loads(result)
    
    def getStatus(self, sessionId, datafileIds=[], datasetIds=[], investigationIds=[]):
        """
        Return the status of the data specified by the datafileIds datasetIds and investigationIds
        """
        parameters = {}
        if sessionId: parameters["sessionId"] = sessionId;   
        _fillParms(parameters, datafileIds, datasetIds, investigationIds)
        return  self._process("getStatus", parameters, "GET").read()
    
    def restore(self, sessionId, datafileIds=[], datasetIds=[], investigationIds=[]):
        """
        Restore data specified by the datafileIds datasetIds and investigationIds
        """
        parameters = {"sessionId": sessionId}
        _fillParms(parameters, datafileIds, datasetIds, investigationIds)
        self._process("restore", parameters, "POST").read()
        
    def archive(self, sessionId, datafileIds=[], datasetIds=[], investigationIds=[]):
        """
        Archive data specified by the datafileIds datasetIds and investigationIds
        """
        parameters = {"sessionId": sessionId}
        _fillParms(parameters, datafileIds, datasetIds, investigationIds)
        self._process("archive", parameters, "POST").read()
      
    def isPrepared(self, preparedId):
        """
        Returns true if the data identified by the preparedId returned by a call to prepareData is ready.
        """
        parameters = {"preparedId": preparedId}
        response = self._process("isPrepared", parameters, "GET").read()
        return response.lower() == "true"
    
    def prepareData(self, sessionId, datafileIds=[], datasetIds=[], investigationIds=[], compressFlag=False, zipFlag=False):
        """
        Prepare data for a subsequent getPreparedData call.
        """
        parameters = {"sessionId": sessionId}
        _fillParms(parameters, datafileIds, datasetIds, investigationIds)
        if zipFlag:  parameters["zip"] = "true";
        if compressFlag: parameters["compress"] = "true";
        return self._process("prepareData", parameters, "POST").read()
       
    def getData(self, sessionId, datafileIds=[], datasetIds=[], investigationIds=[], compressFlag=False, zipFlag=False, outname=None, offset=0):
        """
        Stream the requested data - note that the outname parameter is unused and will be removed in the future.
        """
        parameters = {"sessionId": sessionId}
        _fillParms(parameters, datafileIds, datasetIds, investigationIds)
        if zipFlag:  parameters["zip"] = "true";
        if compressFlag: parameters["compress"] = "true";
        if offset: headers = {"Range": "bytes=" + str(offset) + "-"} 
        else: headers = None
        return self._process("getData", parameters, "GET", headers=headers)
    
    def getDatafileIds(self, sessionId, datafileIds=[], datasetIds=[], investigationIds=[]):
        """
        Return the list of data file id values specified.
        """
        parameters = {"sessionId": sessionId}
        _fillParms(parameters, datafileIds, datasetIds, investigationIds)
        result = self._process("getDatafileIds", parameters, "GET").read()
        return json.loads(result)["ids"]
    
    def getLink(self, sessionId, datafileId):
        """
        Return a hard link to a data file
      
        This is only useful in those cases where the user has direct access to the file system where
        the IDS is storing data. The caller is only granted read access to the file.
        """
        parameters = {"sessionId": sessionId, "datafileId" : datafileId, "username": getpass.getuser() }
        return self._process("getLink", parameters, "POST").read()
    
    def getSize(self, sessionId, datafileIds=[], datasetIds=[], investigationIds=[]):
        """
        Return the total size of the datafiles
        """
        parameters = {"sessionId": sessionId}
        _fillParms(parameters, datafileIds, datasetIds, investigationIds)
        return long(self._process("getSize", parameters, "GET").read())
    
    def getDataUrl(self, sessionId, datafileIds=[], datasetIds=[], investigationIds=[], compressFlag=False, zipFlag=False, outname=None):
        """
        Get URL to retrieve the requested data
        """
        parameters = {"sessionId": sessionId}
        _fillParms(parameters, datafileIds, datasetIds, investigationIds)
        if zipFlag:  parameters["zip"] = "true";
        if compressFlag: parameters["compress"] = "true";
        if outname: parameters["outname"] = outname
        return self._getDataUrl(parameters)
    
    def getIcatUrl(self):
        """
        Get URL of the ICAT to which the IDS authorizes operations on the basis of a sessionId
        """
        return self._process("getIcatUrl", {}, "GET").read()
    
    def delete(self, sessionId, datafileIds=[], datasetIds=[], investigationIds=[]):
        """
        Delete the data identified by the datafileIds, datasetIds and investigationIds
        """
        parameters = {"sessionId": sessionId}
        _fillParms(parameters, datafileIds, datasetIds, investigationIds)
        self._process("delete", parameters, "DELETE")
    
    def getPreparedData(self, preparedId, outname=None, offset=0):
        """
        Get the data using the preparedId returned by a call to prepareData
        """
        parameters = {"preparedId": preparedId}
        if outname: parameters["outname"] = outname
        if offset: headers = {"Range": "bytes=" + str(offset) + "-"}
        else: headers = None
        return self._process("getData", parameters, "GET", headers=headers)
    
    def getPreparedDatafileIds(self, preparedId):
        """
        Get the list of data file id values using the preparedId returned by a call to prepareData
        """
        parameters = {"preparedId": preparedId}
        result = self._process("getDatafileIds", parameters, "GET").read()
        return json.loads(result)["ids"]
    
    def getPreparedDataUrl(self, preparedId, outname=None):
        """
        Get the URL to retrieve data using the preparedId returned by a call to prepareData
        """
        parameters = {"preparedId": preparedId}
        if outname: parameters["outname"] = outname
        return self._getDataUrl(parameters)
      
    def put(self, sessionId, inputStream, name, datasetId,
            datafileFormatId, description=None, doi=None, datafileCreateTime=None,
             datafileModTime=None):
        """
        Put the data in the inputStream into a data file and catalogue it. The client generates a
        checksum which is compared to that produced by the server to detect any transmission errors.
        """
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
    
    def _getDataUrl(self, parameters):
        if self.secure:
            url = "https://"
        else:
            url = "http://"
        url = url + self.ids_host + self.path + "getData" + "?" + urlencode(parameters)
        if len(url) > 2048: raise IdsException(400, "Generated URL is of length "
                        + len(url) + " which exceeds 2048")
        return url
         
    def _process(self, relativeUrl, parameters, method, headers=None, body=None):
        path = self.path + relativeUrl
        if parameters: parameters = urlencode(parameters)
        if parameters and method != "POST":
            path = path + "?" + parameters
        urllen = 4 + len(path) + len(self.ids_host)
        if self.secure:
            conn = httplib.HTTPSConnection(self.ids_host)
            urllen += 5
        else:
            conn = httplib.HTTPConnection(self.ids_host)
            urllen += 4
        if urllen > 2048: raise IdsException(400, "Generated URI is of length "
                        + urllen + " which exceeds 2048")
             
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
                conn.putheader(header, headers[header])
                
        if parameters and method == "POST":
            conn.putheader('Content-Type', 'application/x-www-form-urlencoded')       
                
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
            try:
                responseContent = response.read()
                om = json.loads(responseContent)
            except Exception:
                raise IdsException("InternalException", responseContent)
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
     

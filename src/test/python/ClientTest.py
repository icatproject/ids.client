import unittest
import ids
from suds.client import Client
import logging
import tempfile
import os
import shutil

logging.basicConfig(level=logging.CRITICAL)

class ClientTest(unittest.TestCase):

    def setUp(self):
        idsUrl = "https://smfisher.esc.rl.ac.uk:8181"
        icatUrl = "https://smfisher.esc.rl.ac.uk:8181"
        plugin = "db"
        creds = "username root password password"
        
        client = Client(icatUrl + "/ICATService/ICAT?wsdl")
        service = client.service
        factory = client.factory
        credentials = factory.create("credentials")
        creds = creds.split()
        for i in range (0, len(creds) - 1, 2):
            entry = factory.create("credentials.entry")
            entry.key = creds[i]   
            entry.value = creds[i + 1]
            credentials.entry.append(entry)
        self.sessionId = service.login(plugin, credentials)
        self.client = ids.IdsClient(idsUrl)
        
        facilities = service.search(self.sessionId, "Facility [name = 'IdsPythonClientTest']")
        if facilities:
            service.deleteMany(self.sessionId, facilities)
        facility = factory.create("facility")
        facility.name = "IdsPythonClientTest"
        facility.id = service.create(self.sessionId, facility)
        itype = factory.create("investigationType")
        itype.facility = facility
        itype.name = "fred"
        itype.id = service.create(self.sessionId, itype)
        inv = factory.create("investigation")
        inv.facility = facility
        inv.name = "fred"
        inv.visitId = "42"
        inv.title = "Dr Frederick Fred"
        inv.type = itype
        inv.id = service.create(self.sessionId, inv)
        dtype = factory.create("datasetType")
        dtype.facility = facility
        dtype.name = "fred"
        dtype.id = service.create(self.sessionId, dtype)
        self.dataset = factory.create("dataset")
        self.dataset.investigation = inv
        self.dataset.name = "fred"
        self.dataset.type = dtype
        self.dataset.id = service.create(self.sessionId, self.dataset)
        self.dformat = factory.create("datafileFormat")
        self.dformat.facility = facility
        self.dformat.name = "fred"
        self.dformat.version = "4.2.0"
        self.dformat.id = service.create(self.sessionId, self.dformat)
        
    def testGetServiceStatus(self): 
        status = self.client.getServiceStatus(self.sessionId)
        self.assertFalse(status["opsQueue"])
        self.assertFalse(status["prepQueue"])

    def testGetStatus(self):
        try:
            self.client.getStatus(self.sessionId)
            self.fail("Should have thrown exception")
        except ids.IdsException as e:
            self.assertEqual("BadRequestException", e.code)   
            
    def testGetStatus2(self):
        try:
            self.client.getStatus(self.sessionId, datafileIds=[1, 2, 3])
            self.fail("Should have thrown exception")
        except ids.IdsException as e:
            self.assertEqual("NotFoundException", e.code)

  
    def testRestore(self):
        try:
            self.client.restore(self.sessionId, datafileIds=[1, 2, 3])
            self.fail("Should have thrown exception")
        except ids.IdsException as e:
            print e
            self.assertEqual("NotFoundException", e.code)
            
    def testArchive(self):
        try:
            self.client.archive(self.sessionId)
            self.fail("Should have thrown exception")
        except ids.IdsException as e:
            self.assertEqual("BadRequestException", e.code)   

    def testGetData(self):
        try:
            self.client.getData(self.sessionId, zipFlag=True, outname="fred", offset=50)
            self.fail("Should have thrown exception")
        except ids.IdsException as e:
            self.assertEqual("BadRequestException", e.code)  
 
    def testIsPrepared(self):
        try:
            preparedId = self.sessionId
            self.client.isPrepared(preparedId)
            self.fail("Should have thrown exception")
        except ids.IdsException as e:
            self.assertEqual("NotFoundException", e.code)
            
    def testGetPreparedData(self):
        try:
            preparedId = self.sessionId
            self.client.getPreparedData(preparedId)
            self.fail("Should have thrown exception")
        except ids.IdsException as e:
            self.assertEqual("NotFoundException", e.code) 
            
    def testDelete(self):
        try:
            preparedId = self.sessionId
            self.client.delete(self.sessionId, datafileIds=[1])
            self.fail("Should have thrown exception")
        except ids.IdsException as e:
            self.assertEqual("NotFoundException", e.code)   

    def testPing(self):
        self.client.ping();
 
    def testManyThings(self):
        f = open("a.b", "w")
        wibbles = ""
        for i in range(1000): wibbles += (str(i) + " wibble ")
        f.write(wibbles)
        f.close()
        f = open("a.b")
        dfid = self.client.put(self.sessionId, f, "fred", self.dataset.id, self.dformat.id, "Description")
        f.close()
        self.assertEquals("ONLINE", self.client.getStatus(self.sessionId, datafileIds=[dfid]))
        f = self.client.getData(self.sessionId, datafileIds=[dfid])
        self.assertEquals(wibbles, f.read())
        f.close()
        pid = self.client.prepareData(self.sessionId, datafileIds=[dfid])
        f = self.client.getPreparedData(pid)
        self.assertEquals(wibbles, f.read())
        f.close()
        self.client.delete(self.sessionId, datafileIds=[dfid])
        try:
            self.assertEquals("ONLINE", self.client.getStatus(self.sessionId, datafileIds=[dfid]))
            self.fail("Should have thrown exception")    
        except ids.IdsException as e:
            self.assertEqual("NotFoundException", e.code)   

if __name__ == '__main__':
    unittest.main()

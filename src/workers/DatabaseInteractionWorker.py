from multiprocessing.connection import Connection

from pymongo import MongoClient
import asyncio
from utils.log import log
from utils.handleMessage import sendMessage
import time


from .Worker  import Worker
class DatabaseInteractionWorker(Worker):
  #################
  # dont edit this part
  ################
  _instanceId: str    
  _isBusy: bool = False
  _client: MongoClient 
  _db: str 
  def __init__(self, conn: Connection, config: dict):
    self.conn=conn
    self._db = config.get("database", "mydatabase") 
    self.connection_string = config.get("connection_string", "mongodb://localhost:27017/") 

  def run(self) -> None:
    self._instanceId = "DatabaseInteractionWorker"
    self._client = MongoClient(self.connection_string)
    self._db= self._client[self._db]
    if not self._client:
      log("Failed to connect to MongoDB", "error")
    log(f"Connected to MongoDB at {self.connection_string}", "success")
    asyncio.run(self.listen_task())
    self.health_check()

  
  def health_check(self) -> None:
    while True :
      pass
      sendMessage(conn=self.conn,messageId=self._instanceId, status="healthy")
      time.sleep(10)
  
  async def listen_task(self) -> None:
    while True:
      try:
          if self.conn.poll(1):  # Check for messages with 1 second timeout
              message = self.conn.recv()
              dest = [
                  d
                  for d in message["destination"]
                  if d.split("/", 1)[0] == "DatabaseInteractionWorker"
              ]
              # dest = [d for d in message['destination'] if d ='DatabaseInteractionWorker']
              destSplited = dest[0].split('/')
              method = destSplited[1]
              param= destSplited[2]
              instance_method = getattr(self,method)
              result = instance_method(param)
              print(f"Received message: {result}")
              sendMessage(
                  conn=self.conn, 
                  status="completed",
                  destination=result["destination"],
                  messageId=message["messageId"],
                  data=convertObjectIdToStr(result.get('data', [])),
              )
      except EOFError:
          log("Connection closed by supervisor",'error')
          break
      except Exception as e:
          log(f"Message loop error: {e}",'error')
          break
  
  #########################################
  # Methods for Database Interaction
  #########################################
  
  def getData(self,id):
    if not self._isBusy:
      self._isBusy =True
      collection= self._db['mycollection']
      data = list(collection.find({"project_id":id}))
      
      self._isBusy= False
      return {"data":data,"destination":["RestApiWorker/onProcessed"]}
      
    
  
############### Helper function to convert ObjectId to string in a list of documents
  
  
def convertObjectIdToStr(data: list) -> list:
   res =[]
   for doc in data:
      doc["_id"] = str(doc["_id"])
      res.append(doc)
   return res
# This is the main function that the supervisor calls


def main(conn: Connection, config: dict):
    """Main entry point for the worker process"""
    worker = DatabaseInteractionWorker(conn, config)
    worker.run()
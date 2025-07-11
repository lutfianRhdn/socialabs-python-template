from multiprocessing.connection import Connection
import threading
import uuid
import time
from  utils.log import log 
from utils.handleMessage import sendMessage, convertMessage

from .Worker import Worker

class TemplateWorker(Worker):
    ###############
    # dont edit this part
    ###############
    route_base = "/"
    conn:Connection
    requests: dict = {}
    def __init__(self):
        # we'll assign these in run()
        self._port: int = None

        self.requests: dict = {}
        
    def run(self, conn: Connection, port: int):
        # assign here
        TemplateWorker.conn = conn

        #### add your worker initialization code here
        
        
        
        
        
        
        #### until this part
        # start background threads *before* blocking server
        threading.Thread(target=self.listen_task, daemon=True).start()
        threading.Thread(target=self.health_check, daemon=True).start()

        # asyncio.run(self.listen_task())
        self.health_check()


    def health_check(self):
        """Send a heartbeat every 10s."""
        while True:
            sendMessage(
                conn=TemplateWorker.conn,
                messageId="heartbeat",
                status="healthy"
            )
            time.sleep(10)
    def listen_task(self):
        while True:
            try:
                if TemplateWorker.conn.poll(1):  # Check for messages with 1 second timeout
                    message = self.conn.recv()
                    dest = [
                        d
                        for d in message["destination"]
                        if d.split("/", 1)[0] == "TemplateWorker"
                    ]
                    destSplited = dest[0].split('/')
                    method = destSplited[1]
                    param= destSplited[2]
                    instance_method = getattr(self,method)
                    instance_method(message)
            except EOFError:
                break
            except Exception as e:
              print(e)
              log(f"Listener error: {e}",'error' )
              break

    def sendToOtherWorker(self, destination, messageId: str, data: dict = None) -> None:
      sendMessage(
          conn=TemplateWorker.conn,
          destination=destination,
          messageId=messageId,
          status="completed",
          reason="Message sent to other worker successfully.",
          data=data or {}
      )
    ##########################################
    # add your worker methods here
    ##########################################
    def test(self,message)->None:
        """
        Example method to test the worker functionality.
        Replace this with your actual worker methods.
        """
        data = message.get("data", {})


        # process


        #send back to RestAPI
        self.sendToOtherWorker(
          messageId=message.get("messageId"),
          destination=["RestApiWorker/onProcessed"],
          data=data
          )
      #   sendMessage(
      #     status="completed",
      #     reason="Test method executed successfully.",
      #     destination=["supervisor"],
      #     data={"message": "This is a test response."}
      # )
        log("Test method called", "info")
        # return {"status": "success", "data": "This is a test response."}

def main(conn: Connection, config: dict):
    worker = TemplateWorker()
    worker.run(conn, config)

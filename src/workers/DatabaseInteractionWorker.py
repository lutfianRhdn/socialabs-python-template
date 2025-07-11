from multiprocessing.connection import Connection
import threading
import uuid
import time
import utils.log as log
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
                    raw = TemplateWorker.conn.recv()
                    msg = convertMessage(raw)
                    self.onProcessed(raw)
            except EOFError:
                break
            except Exception as e:
                log(f"Listener error: {e}",'error' )
                break


    ##########################################
    # add your worker methods here
    ##########################################
   

def main(conn: Connection, config: dict):
    worker = TemplateWorker()
    worker.run(conn, config)

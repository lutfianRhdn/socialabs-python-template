
import sys
import os
import time
import threading
import importlib
import multiprocessing
from datetime import datetime
from multiprocessing.connection import Connection
from utils.log import log
from utils.handleMessage import sendMessage,convertMessage


#########
# dont edit this class except worker conf
#########
class Supervisor:
    _workers:dict={}
    workers_health:dict ={}
    pending_messages:dict={}
    
    def __init__(self):

        ####
        # just edit this part to add your workers
        ####
        
        self.create_worker("DatabaseInteractionWorker", count=1, config={
          'connection_string': 'mongodb://localhost:27017/',
          'database': 'mydatabase'
        })
        
        self.create_worker("RestApiWorker", count=1, config={
          'port':8000
        })
        
        self.create_worker("TemplateWorker", count=1, config={
          # Add any configuration needed for your worker here
          'example_config': 'value'
        })
        
        ####
        # until this part
        ####
        

        # Start periodic health check thread
        self._health_thread = threading.Thread(target=self._health_loop, daemon=True)
        self._health_thread.start()
        log("Supervisor initialized", "info")

    def create_worker(self, worker: str, count: int = 1, config: dict = None):
        if count <= 0:
            log("Worker count must be greater than zero", "error")
            raise ValueError("Worker count must be greater than zero")
        config = config or {}
        log(f"Creating {count} worker(s) of type {worker}", "info")

        for _ in range(count):
            parent_conn, child_conn = multiprocessing.Pipe()
            
            p = multiprocessing.Process(
                target=Supervisor._worker_runner,
                args=(worker, child_conn, config),  
                daemon=True
            )
            p.start()
            self._workers[p.pid] = {"process": p, "conn": parent_conn, "name": worker}
            self._start_listener(p.pid)
            
        running = list([pid for pid, info in self._workers.items() if info['process'].is_alive() and info['name'] == worker])
        log(f"{worker} running on pid(s): {running}", "success")
        self.resend_pending_messages(worker)
    @staticmethod
    def _worker_runner( worker_name: str, conn: Connection, config: dict):
        try:
            module = importlib.import_module(f"workers.{worker_name}")
            module.main(conn, config)
        except ModuleNotFoundError as e:
            log(f"Worker module not found: workers.{worker_name}", "error")
        except Exception as e:
            log(f"Worker error: {e}", "error")
        finally:
            conn.close()

    def _start_listener(self, pid: int):
        def listen():
            conn = self._workers[pid]["conn"]
            while True:
                try:
                    message = conn.recv()
                    self.handle_worker_message(convertMessage(message), pid)
                except EOFError as e:
                    log(f"Connection closed for worker {pid}", "warn")
                    break
                except Exception as e:
                    log(f"Error listening to worker {pid}: {e}", "error")
                    break
        t = threading.Thread(target=listen, daemon=True)
        t.start()

    def _health_loop(self):
        while True:
            time.sleep(10)
            self.check_worker_health()

    def check_worker_health(self):
        now = time.time()
        threshold = 15
        for pid, health in list(self.workers_health.items()):
            if health['is_healthy'] and now - health['timestamp'] > threshold:
                log(f"Worker {pid} not healthy, restarting...", "warn")
                self._kill_worker(pid)
                self.create_worker(health['worker_name'], count=1, config={})
                del self.workers_health[pid]

    def handle_worker_message(self, message: dict, pid: int):
        dests = message.get('destination')
        status = message.get('status')
        msg_id = message.get('messageId')
        for dest in dests:
          if dest != 'supervisor':
              self._send_to_worker(dest, message)
              return

        # Supervisor-specific handling
        if status == 'healthy':
            instance = message.get('messageId', '')
            worker_name = instance.split('-')[0] if instance else ''
            self.workers_health[pid] = {
                'is_healthy': True,
                'worker_name': worker_name,
                'timestamp': time.time()
            }
        elif status == 'completed' and dest:
            worker_name = dest.split('/')[0].split('.')[0]
            self.remove_pending_message(worker_name, msg_id)

    def _send_to_worker(self, destination: str, message: dict):
        worker_name = destination.split('/')[0].split('.')[0]
        msg_id = message.get('messageId')
        status = message.get('status')
        reason = message.get('reason')

        log(f"Routing message {msg_id} to {worker_name}", "info")
        self.track_pending_message(worker_name, message)

        # Find available worker
        available = [w for w in self._workers.values() if w['name'] == worker_name]
        if status == 'failed' and reason == 'SERVER_BUSY':
            # filter out busy
            available = []

        if not available:
            log(f"No available worker for destination: {destination}", "warn")
            # retry later - fix the lambda to capture pid properly
            def retry_message():
                self.handle_worker_message({**message, 'status':'completed'}, 0)
            threading.Timer(5, retry_message).start()
            return

        target = available[0]
        try:
            target['conn'].send(message)
            log(f"Sent message {msg_id} to {worker_name} PID: {target['process'].pid}", "success")
        except Exception as e:
            log(f"Failed to send message to worker {worker_name}: {e}", "error")

    def track_pending_message(self, worker_name: str, message: dict):
        self.pending_messages.setdefault(worker_name, []).append(message)

    def remove_pending_message(self, worker_name: str, message_id: str):
        if worker_name in self.pending_messages:
            self.pending_messages[worker_name] = [m for m in self.pending_messages[worker_name] if m.get('messageId') != message_id]

    def resend_pending_messages(self, worker_name: str):
        msgs = self.pending_messages.get(worker_name, [])
        if not msgs:
            return
        log(f"Resending {len(msgs)} pending messages to {worker_name}", "info")
        connections = [w['conn'] for w in self._workers.values() if w['name'] == worker_name]
        if not connections:
            log(f"No connection available for {worker_name}", "warn")
            return
        for msg in msgs:
            try:
                connections[0].send(msg)
                log(f"Resent message {msg.get('messageId')} to {worker_name}", "success")
            except Exception as e:
                log(f"Failed to resend: {e}", "error")

    def _kill_worker(self, pid: int):
        info = self._workers.pop(pid, None)
        if info:
            try:
                info['conn'].close()
                info['process'].terminate()
            except Exception as e:
                log(f"Error terminating worker {pid}: {e}", "error")

    def is_worker_alive(self, pid: int) -> bool:
        info = self._workers.get(pid)
        return info is not None and info['process'].is_alive()


if __name__ == '__main__':
    # Add this for Windows multiprocessing support
    multiprocessing.set_start_method('spawn', force=True)
    
    supervisor = Supervisor()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log("Shutting down Supervisor", "info")
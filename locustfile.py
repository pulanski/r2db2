from locust import User, task, between
import socket
import time
import random

class TcpClient(User):
    wait_time = between(1, 2)

    def on_start(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def on_stop(self):
        self.sock.close()

    @task
    def connect_to_server(self):
        try:
            self.sock.connect(("127.0.0.1", 2345))
            # Sleep for 1-10 seconds to simulate a request/response
            time.sleep(random.randint(1, 10))

            # Exit the connection
            self.sock.close()
        except socket.error as e:
            time.sleep(1)
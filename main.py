import json
import queue
import threading
import time

import pika


class ImageMQClient:
    def _init_(self, config):
        self.credentials = pika.PlainCredentials(config["username"], config["password"])
        self.parameters = pika.ConnectionParameters(
            host=config["host"], port=config["port"], virtual_host="/", credentials=self.credentials
        )
        self.connection = pika.BlockingConnection(self.parameters)
        self.channel = self.connection.channel()
        self.taskID = config["taskID"]
        self.messageType = config["messageType"]

        self.message_queue = queue.Queue()  # 创建消息队列
        self.stop_event = threading.Event()  # 创建停止事件
        self.worker_thread = threading.Thread(target=self._process_queue)  # 创建工作线程
        self.worker_thread.start()  # 启动工作线程

    def _encode_message(self, binary_data=None, binary_type=None):
        raw_data = dict({
            "json": {
                "taskID": self.taskID,
                "messageType": self.messageType,
                "binary": []
            },
            "binary": []
        })

        if binary_data is not None:
            if binary_type is not None:
                for i in range(0, len(binary_data)):
                    raw_data["json"]["binary"].append({
                        "length": f"{len(binary_data[i])}",  # 使用 len 获取实际字节长度
                        "type": f"{binary_type}",
                        "timestamp": f"{time.time()}"
                    })
                    raw_data["binary"].append(binary_data[i])
            else:
                print("No binary data type provided")

        json_str = json.dumps(raw_data["json"])
        json_bytes = json_str.encode("utf-8")

        partA = len(json_bytes).to_bytes(4, byteorder='little', signed=False)
        partB = json_bytes
        partC = b''.join(raw_data["binary"]) if binary_data is not None else b''

        return partA + partB + partC

    def _decode_message(self, encoded_data):
        json_length = int.from_bytes(encoded_data[:4], byteorder='little', signed=False)
        json_bytes = encoded_data[4:4 + json_length]
        json_str = json_bytes.decode("utf-8")

        json_data = json.loads(json_str)
        binary_data = encoded_data[4 + json_length:]

        raw_data = {
            "json": json_data,
            "binary": []
        }

        offset = 0
        for binary_info in json_data["binary"]:
            length = int(binary_info["length"])
            raw_data["binary"].append(binary_data[offset:offset + length])
            offset += length

        return raw_data

    def publish(self, binary_data=None, binary_type=None):
        message = self._encode_message(binary_data, binary_type)
        self.message_queue.put(message)  # 将消息放入队列

    def _process_queue(self):
        while not self.stop_event.is_set():
            try:
                msg = self.message_queue.get(timeout=1)  # 从队列中获取消息
                self.channel.exchange_declare(exchange=self.taskID, exchange_type="direct")
                self.channel.basic_publish(
                    exchange=self.taskID,
                    routing_key=self.messageType,
                    body=msg,
                    properties=pika.BasicProperties(
                        headers={
                            "taskID": self.taskID,
                            "messageType": self.messageType
                        }),
                )
                self.message_queue.task_done()
            except queue.Empty:
                continue

    def receive(self):
        def callback(ch, method, properties, body):
            decoded_message = self._decode_message(body)
            print(f"Received message: {decoded_message}")

        self.channel.exchange_declare(exchange=self.taskID, exchange_type="direct")

        self.channel.queue_declare(queue=self.messageType)
        self.channel.queue_bind(exchange=self.taskID, queue=self.messageType, routing_key=self.messageType)

        self.channel.basic_consume(
            queue=self.messageType, on_message_callback=callback, auto_ack=True
        )

        print('Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()

    def close(self):
        self.stop_event.set()  # 设置停止事件
        self.worker_thread.join()  # 等待工作线程结束
        self.connection.close()  # 关闭 RabbitMQ 连接


if __name__ == "__main__":
    config1 = {
        "host": "192.168.50.50",
        "port": 5672,
        "username": "ai601",
        "password": "123456",
        "taskID": "99",
        "messageType": "RawImageMessage",
    }

    test_pic1 = b'test data 001'
    test_pic2 = b'test data 002'
    mqc1 = ImageMQClient(config1)
    mqc1.publish(binary_data=[test_pic1, test_pic2], binary_type="image/jpeg")

    time.sleep(5)  # 等待一段时间以确保消息发送完成
    mqc1.receive()

    mqc1.close()  # 关闭客户端

from flask import Flask, jsonify, request
import hazelcast
import argparse
import threading
from consul_lib import *

import requests

# Парсер аргументів командного рядка для вказання порту
parser = argparse.ArgumentParser()
parser.add_argument("--port", type=int, required=True)
args = parser.parse_args()

# Реєстрація сервісу повідомлень у Consul
service_id = Register_service('message-service', args.port)

# Отримання конфігурації Hazelcast з Consul
hz_config = json.loads(Get_value('HZ_config'))
print("Hazelcast config: ", hz_config)

# Ініціалізація клієнта Hazelcast та отримання блокуючої черги повідомлень
hz = hazelcast.HazelcastClient(cluster_name=hz_config['cluster_name'], cluster_members=[])
messages_queue = hz.get_queue(hz_config['queue_name']).blocking()

# Ініціалізація Flask додатка
app = Flask(__name__)

# Список для зберігання отриманих повідомлень з черги
message_list = []

# Функція, яка виконується в окремому потоці для отримання повідомлень з черги
def queue_event():
    while True:
        item = messages_queue.take()
        message_list.append(item)
        print("Отримано повідомлення з черги:", str(item))

@app.route('/data', methods=['GET', 'POST'])
def data():
    if request.method == 'GET':
        return '\n'.join(message_list)
    else:
        return jsonify({'error': 'Bad request'})

if __name__ == '__main__':
    # Запуск потоку для обробки черги повідомлень
    event_thread = threading.Thread(target=queue_event)
    event_thread.start()
    
    # Запуск Flask додатка на вказаному порту
    app.run(port=args.port)
    
    # Вивід ідентифікатора поточного сервісу у Consul
    print("Consul Current_ID:", service_id)
    input("")
    
    # Видалення реєстрації сервісу після закінчення роботи
    Deregister_service(service_id)

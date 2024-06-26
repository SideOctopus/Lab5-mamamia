from flask import Flask, jsonify, request
import requests
import hazelcast
import argparse

from consul_lib import *

# Парсер аргументів командного рядка для вказання портів
parser = argparse.ArgumentParser()
parser.add_argument("--logport", type=int, required=True)
parser.add_argument("--hzport", type=int, required=True)
args = parser.parse_args()

# Реєстрація сервісу логування у Consul
service_id = Register_service('logging-service', args.logport)

# Отримання конфігурації Hazelcast з Consul
hz_config = json.loads(Get_value('HZ_config'))
print("Hazelcast config: ", hz_config)

# Ініціалізація клієнта Hazelcast та отримання блокуючого словника повідомлень
hz = hazelcast.HazelcastClient(cluster_name=hz_config['cluster_name'], cluster_members=[])
messages_map = hz.get_map(hz_config['map_name']).blocking()

# Ініціалізація Flask додатка
app = Flask(__name__)

# Обробник маршруту '/data' для методів GET та POST
@app.route('/data', methods=['GET', 'POST'])
def data():
    if request.method == "POST":
        key = request.form['key']
        message = request.form['msg']
        messages_map.put(key, message)
        print("Received message via POST:", key, message)
        return ("Success!")
    elif request.method == "GET":
        keys = messages_map.key_set()
        messages = []
        for key in keys:
            message = messages_map.get(key)
            messages.append(message)
        return "\n".join(messages)

if __name__ == '__main__':
    # Запуск Flask додатка на вказаному порту для логування
    app.run(port=args.logport)
    
    # Вивід ідентифікатора поточного сервісу у Consul
    print("Consul Current_ID:", service_id)
    input("")
    
    # Видалення реєстрації сервісу після завершення роботи
    Deregister_service(service_id)

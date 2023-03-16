#     Copyright 2022. ThingsBoard
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

import socket
from time import sleep
from queue import Queue
from random import choice
from string import ascii_lowercase
from threading import Thread
from thingsboard_gateway.connectors.connector import Connector, log  # Import base class for connector and logger
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader

SOCKET_TYPE = {
    'TCP': socket.SOCK_STREAM,
    'UDP': socket.SOCK_DGRAM
}

class CustomMCDISocketConnector(Thread, Connector):
    def __init__(self, gateway, config, connector_type):
        super().__init__()
        self.__log = log
        self.__config = config
        self._connector_type = connector_type

        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        self.__gateway = gateway
        self.setName(self.__config.get("name",
                                       "Custom %s connector " % self.get_name() + ''.join(
                                           choice(ascii_lowercase) for _ in range(5))))  

        log.info("Starting Custom %s connector", self.get_name())
        self.daemon = True
        self.__stopped = True
        self.__connected = False
        self.__bind = False

        # serial characters delimiters
        self.__end_of_string = str.encode(config.get("endOfString", "\r"))
        self.__send_ack = str.encode(config.get("ACK", "06"))
        self.__alive_character = config.get("aliveCharacter", "@")

        self.__devices = {}

        self.__socket_type = config['type'].upper()
        self.__socket_address = config['bindAddress']
        self.__socket_port = config['bindPort']
        self.__socket_buff_size = config['bufferSize']

        self.__socket = socket.socket(socket.AF_INET, SOCKET_TYPE[self.__socket_type])

        self.__converting_requests = Queue(-1)
        
        self.__load_converters(connector_type)

        log.info('Custom connector %s initialization success.', self.get_name())
        log.info("Devices in configuration file found: %s ", '\n'.join(device for device in self.__devices)) 

        self.__connections = {}

    def open(self):
        self.__stopped = False
        self.start()

    def get_name(self):
        return self.name
    
    def get_config(self):
        return self.__config

    def is_connected(self):
        return self.__connected

    def __load_converters(self, connector_type):
        devices_config = self.__config.get('devices')
        try:
            if devices_config is not None:
                for device_config in devices_config:
                    if device_config.get('converter') is not None:
                        converter = TBModuleLoader.import_module(connector_type, device_config['converter'])
                        self.__devices[device_config['deviceName']] = {'converter': converter(device_config),
                                                                 'device_config': device_config}
                    else:
                        log.error('Converter configuration for the custom connector %s -- not found, please check your configuration file.', self.get_name())
            else:
                log.error('Section "devices" in the configuration not found. A custom connector %s has being stopped.', self.get_name())
                self.close()
        except Exception as e:
            log.exception(e)

    def run(self):  # Main loop of thread
        self._connected = True

        converting_thread = Thread(target=self.__process_data, daemon=True, name='Converter Thread')
        converting_thread.start()

        while not self.__bind:
            try:
                self.__socket.bind((self.__socket_address, self.__socket_port))
            except OSError:
                log.error('Address already in use. Reconnecting...')
                sleep(3)
            else:
                self.__bind = True

        if self.__socket_type == 'TCP':
            self.__socket.listen(5)

        self.__log.info('%s socket is up', self.__socket_type)

        while not self.__stopped:
            try:
                if self.__socket_type == 'TCP':
                    conn, address = self.__socket.accept()
                    self.__connections[address] = conn

                    self.__log.info('New connection %s established', address)
                    thread = Thread(target=self.__process_tcp_connection, daemon=True,
                                    name=f'Processing {address} connection',
                                    args=(conn, address))
                    thread.start()
                else:
                    data, client_address = self.__socket.recvfrom(self.__socket_buff_size)
                    self.__converting_requests.put((client_address, data))
            except ConnectionAbortedError:
                self.__socket.close()

    def __process_tcp_connection(self, connection, address):
        while not self.__stopped:
            data = connection.recv(self.__socket_buff_size)

            if data:
                self.__converting_requests.put((address, data, connection))
            else:
                break

        connection.close()
        self.__connections.pop(address)
        self.__log.debug('Connection %s closed', address)
    
    def __process_data(self):
        while not self.__stopped:
            if not self.__converting_requests.empty():
                (address, port), data, connection = self.__converting_requests.get()

                try:
                    if data.find(self.__end_of_string): # send ACK
                        connection.sendall(self.__send_ack)
                        self.__log.debug(f"Sending ACK at {address}:{port}")
                except Exception as e:
                    self.__log.exception(e)
                    
                self.__convert_data(data)

            sleep(.2)

    def __convert_data(self, data):
        converted_data = {}
        
        for device in self.__devices:
            device_encoding = self.__devices[device]["device_config"]["encoding"]
            try:
                data = data.replace(self.__end_of_string, b'')
                decoded_data = data.decode(device_encoding)
            except Exception as e:
                self.__log.debug(e)
                return None

            if decoded_data.find(self.__alive_character) < 0:
                converted_data = self.__devices[device]['converter'].convert(self.__devices[device]['device_config'], decoded_data)

                try:
                    self.__gateway.send_to_storage(self.get_name(), converted_data)
                    self.__log.info('Data to ThingsBoard %s', converted_data)
                except Exception as e:
                    self.__log.exception(e)

    def close(self): 
        self.__stopped = True
        self._connected = False
        self.__connections = {}

    def on_attributes_update(self, content): 
        pass

    def server_side_rpc_handler(self, content):
        pass

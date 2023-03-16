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

from thingsboard_gateway.connectors.converter import Converter, log


class CustomMCDISocketUplinkConverter(Converter):
    def __init__(self, config):
        self.__config = config
        self.result_dict = {
            'deviceName': config.get('deviceName', 'CustomMCDISocketDevice'),
            'deviceType': config.get('deviceType', 'default'),
            'attributes': [],
            'telemetry': []
            }

    def convert(self, config, data):
        keys = ['attributes', 'telemetry']
        for key in keys:
            self.result_dict[key] = []
            if self.__config.get(key) is not None:
                for config_object in self.__config.get(key):
                    data_to_convert = data.replace('  ', ' ') # remove duplicated spaces
                    if config_object.get('splitDelimiter') is not None:
                        delimiter = config_object.get('splitDelimiter')
                        field = config_object.get('field')
                        data_to_convert = data_to_convert.split(delimiter)[field]
                    if config_object.get('toByte') is not None:
                        to_byte = config_object.get('toByte')
                        if to_byte == -1:
                            to_byte = len(data) - 1
                        data_to_convert = data_to_convert[:to_byte]
                    if config_object.get('fromByte') is not None:
                        from_byte = config_object.get('fromByte')
                        data_to_convert = data_to_convert[from_byte:]
                    converted_data = {config_object['key']: data_to_convert}
                    self.result_dict[key].append(converted_data)
        log.debug("Converted data: %s", self.result_dict)
        return self.result_dict

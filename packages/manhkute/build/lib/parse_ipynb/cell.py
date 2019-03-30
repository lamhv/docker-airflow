import xml.etree.ElementTree as ET


class Cell:
    def __init__(self, params):
        self.cell_type = params['cell_type']
        self.source = params['source']

    def get_source(self):
        return ''.join(self.source)

    def get_cell_type(self):
        return self.cell_type


class XmlCell(Cell):
    def __init__(self, params):
        super().__init__(params)

    def get_root(self):
        xml = self.get_source()
        # print(xml)
        tree = ET.ElementTree(ET.fromstring(xml))
        return tree.getroot()

    def get_prepend(self):
        try:
            return self.get_root().find("prepend").text
        except Exception as e:
            return None

    def get_cell_type(self):
        return self.get_root().find("loc").text

    def get_cell_name(self):
        return self.get_root().find("name").text

    def get_block_type(self):
        try:
            return self.get_root().find("type").text
        except Exception as e:
            return None

    def get_dag(self):
        try:
            return self.get_root().find("dag").text
        except Exception as e:
            return None

    def get_upstreams(self):
        try:
            result = []
            for upstream in self.get_root().iter('airflow_upstream'):
                result.append(upstream.find('name').text)
            return result
        except Exception as e:
            return None

    def get_input_name(self):
        try:
            return self.get_root().find("input").find("name").text
        except Exception as e:
            return None

    def get_groups_name(self):
        try:
            return self.get_root().find("parallel").find("group").text
        except Exception as e:
            return None

    def get_parallel(self):
        try:
            return self.get_root().find("parallel")
        except Exception as e:
            return None


class ScriptCell(Cell):
    def __init__(self, params):
        super().__init__(params)


def cell_factory(params):
    if params['cell_type'] == 'code':
        return ScriptCell(params)
    if params['cell_type'] == 'raw':
        return XmlCell(params)
    if params['cell_type'] == 'markdown':
        return ScriptCell(params)

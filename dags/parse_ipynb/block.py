class Block:
    def __init__(self, name, prepend, block_type):
        self.name = name
        self.prepend = prepend
        self.code = []
        self.block_type = block_type
        self.dag = ''
        self.upstreams = []
        self.parallel = None

    def append_cell(self, cell):
        self.code.append(cell.get_source())

    def get_code(self):
        return '\n'.join(self.code)

    def set_parallel(self, parallel):
        self.parallel = parallel

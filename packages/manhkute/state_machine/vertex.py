class Vertex(object):
    def __init__(self, name, upstreams=[], parallel=None, params=None):
        self.name = name
        self.upstreams = upstreams
        self.parallel = parallel
        self.params = params

    def __str__(self):
        return self.name

    def __eq__(self, other):
        return self.name == other.name

    def add_upstream(self, edge):
        self.upstreams.append(edge)

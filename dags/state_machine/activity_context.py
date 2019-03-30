class ActivityContext:
    def __init__(self, params):
        self.params = params

    def get_params(self, key):
        return self.params[key]

    def set_params(self, params):
        self.params = params
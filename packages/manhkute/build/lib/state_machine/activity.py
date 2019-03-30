class Activity(object):
    def execute(self, context):
        pass


class TapeActivity(Activity):
    def execute(self, context):
        idx = context.params["idx"]
        current = context.params["tape"][idx]
        context.params["current_value"] = current
        context.params["idx"] = idx + 1

from state_machine.activity import Activity


class State(Activity):
    def __init__(self, name):
        self.activities = []
        self.name = name

    def add_activity(self, activity):
        self.activities.append(activity)

    def execute(self, context):
        result = None
        for activity in self.activities:
            result = activity.execute(context=context)
        return result

    def __eq__(self, other):
        return self.name == other.name

    def __str__(self):
        return self.name
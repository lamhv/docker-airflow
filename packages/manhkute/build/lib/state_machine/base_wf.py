from state_machine.activity_context import ActivityContext

from state_machine.state import State
from state_machine.workflow import Workflow
from state_machine.workflow_config import WorkflowConfig


class BaseWF:
    def __init__(self):
        self.context = None
        self.config = None

    def create_config(self):
        pass

    def create_context(self, context):
        self.context = ActivityContext(context)

    def run(self, context):
        self.create_context(context=context)
        self.create_config()
        wf_config = WorkflowConfig()
        wf_config.load(self.config)
        wf = Workflow("dummy", State("init"), wf_config=wf_config)
        return wf.run(context=self.context)
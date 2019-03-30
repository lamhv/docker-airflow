from state_machine.state import State


class Workflow:
    def __init__(self, name, current_state, wf_config):
        self.name = name
        self.current_state = current_state
        self.wf_config = wf_config

    def run(self, context):
        result = None
        while True:
            transitions = self.wf_config.get_transitions(self.current_state)
            for transition in transitions:
                if transition.is_satisfy(context):
                    self.current_state = transition.next_state
                    if self.current_state == State("final"):
                        print("Final")
                        return result
                    next_state = self.wf_config.state_factory(self.current_state)
                    try:
                        result = next_state.execute(context)
                    except Exception as e:
                        print(e)
                        break
                    break

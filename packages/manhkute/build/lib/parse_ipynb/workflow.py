from state_machine.activity import TapeActivity
from state_machine.base_wf import BaseWF
from parse_ipynb.block import Block
from parse_ipynb.cell import ScriptCell
from state_machine.state import State


class ReadCell(TapeActivity):
    def execute(self, context):
        super().execute(context)


class CreateBlock(TapeActivity):
    def execute(self, context):
        super().execute(context)
        block = Block(name=context.params['current_value'].get_cell_name(),
                      prepend=context.params['current_value'].get_prepend(),
                      block_type=context.params["current_value"].get_block_type())
        block.dag = context.params['current_value'].get_dag()
        block.upstreams = context.params['current_value'].get_upstreams()

        context.params['current_block'] = block

        input_name = context.params["current_value"].get_input_name()
        if input_name is not None:
            params = \
                {
                    "cell_type": "code",
                    "source": ['\n# auto generated\ndate = str(sys.argv[1])\n', input_name + ' = [date]\n']
                }
            input_cell = ScriptCell(params)
            context.params['current_block'].append_cell(input_cell)

        groups_name = context.params['current_value'].get_groups_name()
        if groups_name is not None:
            params = \
                {
                    "cell_type": "code",
                    "source": ['\n# auto generated', '\nstart = int(sys.argv[2])', '\nmod = int(sys.argv[3])', '\n{input_group_list} = range(start, 20, mod)'.format(input_group_list=groups_name), '\n# end auto generated\n']
                }
            groups_cell = ScriptCell(params)
            context.params['current_block'].append_cell(groups_cell)

        parallel = context.params['current_value'].get_parallel()
        if parallel is not None:
            context.params['current_block'].set_parallel({"mod": int(parallel.find("mod").text)})


class ReadCellAppend(TapeActivity):
    def execute(self, context):
        super().execute(context)
        context.params["current_block"].append_cell(context.params["current_value"])


class CloseBlock(TapeActivity):
    def execute(self, context):
        super().execute(context)
        context.params["final_blocks"][context.params["current_block"].name] = context.params["current_block"]
        context.params["current_block"] = None


class ParserWorkflow(BaseWF):

    @staticmethod
    def cdt_true(context):
        return True

    @staticmethod
    def cdt_null(context):
        if len(context.params["tape"]) <= (context.params["idx"]):
            return True
        return False

    @staticmethod
    def cdt_code(context):
        cell = context.params["tape"][context.params["idx"]]
        if cell.get_cell_type() == 'code':
            return True
        return False

    @staticmethod
    def cdt_markdown(context):
        cell = context.params["tape"][context.params["idx"]]
        if cell.get_cell_type() == 'markdown':
            return True
        return False

    @staticmethod
    def cdt_start(context):
        cell = context.params["tape"][context.params["idx"]]
        if cell.get_cell_type() == 'start':
            return True
        return False

    @staticmethod
    def cdt_end(context):
        cell = context.params["tape"][context.params["idx"]]
        if cell.get_cell_type() == 'end':
            return True
        return False

    def __init__(self):
        super().__init__()

    def create_config(self):
        self.config = \
            [
                {
                    "state": State("init"),
                    "transitions": [
                        {"next_state": State("final"), "condition": ParserWorkflow.cdt_null},
                        {"next_state": State("1"), "condition": ParserWorkflow.cdt_code},
                        {"next_state": State("1"), "condition": ParserWorkflow.cdt_markdown},
                        {"next_state": State("1"), "condition": ParserWorkflow.cdt_end},
                        {"next_state": State("2"), "condition": ParserWorkflow.cdt_start}
                    ],
                    "activities": [ReadCell()]
                },
                {
                    "state": State("1"),
                    "transitions": [
                        {"next_state": State("final"), "condition": ParserWorkflow.cdt_null},
                        {"next_state": State("1"), "condition": ParserWorkflow.cdt_code},
                        {"next_state": State("1"), "condition": ParserWorkflow.cdt_markdown},
                        {"next_state": State("1"), "condition": ParserWorkflow.cdt_end},
                        {"next_state": State("2"), "condition": ParserWorkflow.cdt_start}
                    ],
                    "activities": [ReadCell()]
                },
                {
                    "state": State("2"),
                    "transitions": [
                        {"next_state": State("final"), "condition": ParserWorkflow.cdt_null},
                        {"next_state": State("3"), "condition": ParserWorkflow.cdt_code},
                        {"next_state": State("5"), "condition": ParserWorkflow.cdt_markdown},
                        {"next_state": State("4"), "condition": ParserWorkflow.cdt_end},
                        {"next_state": State("2"), "condition": ParserWorkflow.cdt_start}
                    ],
                    "activities": [CreateBlock()]
                },
                {
                    "state": State("3"),
                    "transitions": [
                        {"next_state": State("final"), "condition": ParserWorkflow.cdt_null},
                        {"next_state": State("3"), "condition": ParserWorkflow.cdt_code},
                        {"next_state": State("5"), "condition": ParserWorkflow.cdt_markdown},
                        {"next_state": State("4"), "condition": ParserWorkflow.cdt_end},
                        {"next_state": State("5"), "condition": ParserWorkflow.cdt_start}
                    ],
                    "activities": [ReadCellAppend()]
                },
                {
                    "state": State("4"),
                    "transitions": [
                        {"next_state": State("final"), "condition": ParserWorkflow.cdt_null},
                        {"next_state": State("1"), "condition": ParserWorkflow.cdt_code},
                        {"next_state": State("1"), "condition": ParserWorkflow.cdt_markdown},
                        {"next_state": State("1"), "condition": ParserWorkflow.cdt_end},
                        {"next_state": State("2"), "condition": ParserWorkflow.cdt_start}
                    ],
                    "activities": [CloseBlock()]
                },
                {
                    "state": State("5"),
                    "transitions": [
                        {"next_state": State("final"), "condition": ParserWorkflow.cdt_null},
                        {"next_state": State("3"), "condition": ParserWorkflow.cdt_code},
                        {"next_state": State("5"), "condition": ParserWorkflow.cdt_markdown},
                        {"next_state": State("4"), "condition": ParserWorkflow.cdt_end},
                        {"next_state": State("5"), "condition": ParserWorkflow.cdt_start}
                    ],
                    "activities": [ReadCell()]
                }
            ]

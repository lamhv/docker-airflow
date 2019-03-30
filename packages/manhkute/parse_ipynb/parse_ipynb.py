import json
import sys

from parse_ipynb.cell import cell_factory
from parse_ipynb.workflow import ParserWorkflow
from state_machine.vertex import Vertex


def save(blocks):
    for b in blocks:
        if blocks[b].block_type == 'script':
            output = []
            output.append(blocks[blocks[b].prepend].get_code())
            output.append(blocks[b].get_code())
            output.append("\n# auto generated\nsys.exit(airflow_code)")
            with open("{path}/{name}.py".format(name=blocks[b].name, path=target_path), "w") as f:
                f.write('\n'.join(output))


def save_upstreams(blocks):
    print("save upstreams")
    for b in blocks:
        if blocks[b].block_type == 'script':

            if blocks[b].upstreams is not None and len(blocks[b].upstreams) > 0:
                vertex = Vertex(name=blocks[b].name, upstreams=blocks[b].upstreams, parallel=blocks[b].parallel)
            else:
                vertex = Vertex(name=blocks[b].name, parallel=blocks[b].parallel)
            try:
                with open("{path}/{name}.json".format(path=target_dag, name=blocks[b].dag), "r") as f:
                    current_data = f.read()
                    # print(current_data)
                    json_object = json.loads(current_data)
                    json_object['tasks'].append(vertex.__dict__)
            except FileNotFoundError:
                json_object = {
                    "tasks": [vertex.__dict__]
                }

            with open("{path}/{name}.json".format(path=target_dag, name=blocks[b].dag), "w") as f:
                print("{path}/{name}.json".format(path=target_dag, name=blocks[b].dag))
                # print(json.dumps(json_object, indent=4))
                f.write(json.dumps(json_object, indent=4))
                print("Writing completed")


if __name__ == '__main__':
    file_name = str(sys.argv[1])
    target_path = str(sys.argv[2])
    target_dag = str(sys.argv[3])

    with open(file_name) as file:
        data = file.read()
    json_cells = json.loads(data)['cells']
    cells = []
    for json_cell in json_cells:
        cells.append(cell_factory(json_cell))

    context = {
        "tape": cells,
        "idx": 0,
        "current_block": None,
        "final_blocks": {},
        "current_value": None
    }

    workflow = ParserWorkflow()
    workflow.run(context=context)

    save(context['final_blocks'])
    save_upstreams(context['final_blocks'])

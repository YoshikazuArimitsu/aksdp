from logging import getLogger
from aksdp.graph import Graph, TaskStatus
import time
import zlib
import base64

logger = getLogger(__name__)


class PlantUML:
    @classmethod
    def graph_to_plantuml(cls, graph: Graph) -> str:
        components = [f"[{t.task.__class__.__name__}]" for t in graph.graph]

        dependencies = []
        for g in graph.graph:
            _to = g.task.__class__.__name__
            _to_datakeys = g.task.input_datakeys()

            if g.dependencies:
                for t in g.dependencies:
                    _from = t.task.__class__.__name__
                    _from_datakeys = t.task.output_datakeys()

                    # I/Oの datakey が宣言されていれば注釈追加
                    datakeys = list(set(_to_datakeys) & set(_from_datakeys))
                    datamemo = ""
                    if datakeys:
                        datamemo = f" : {','.join(datakeys)}"

                    dependencies.append(f"{_from} --> {_to}{datamemo}")

        _components = "\n".join(components)
        _dependencies = "\n".join(dependencies)
        uml = f"""
@startuml
{_components}
{_dependencies}
@enduml
"""
        return uml

    @classmethod
    def graph_to_plantuml_textenc(cls, graph: Graph) -> str:
        s = PlantUML.graph_to_plantuml(graph)
        compressed = zlib.compress(s.encode("utf-8"))
        encoded = base64.b64encode(compressed[2:-4])

        puml_base64chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_".encode("utf-8")
        std_base64chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".encode("utf-8")

        s = encoded.translate(bytes.maketrans(std_base64chars, puml_base64chars)).decode("utf-8")

        return s

    @classmethod
    def graph_to_url(cls, graph: Graph, base_url: str = "http://www.plantuml.com/plantuml/png/") -> str:
        return f"{base_url}{PlantUML.graph_to_plantuml_textenc(graph)}"

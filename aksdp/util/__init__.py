from .plantuml import PlantUML as PlantUML

try:
    from .airflow import AirFlow as AirFlow
except ImportError:
    print("airflow import error")
    pass

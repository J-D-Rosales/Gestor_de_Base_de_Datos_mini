from abc import ABC, abstractmethod

class BaseIndex(ABC):
    def __init__(self, table_name):
        self.table_name = table_name

    def _format_result(self, data, io_cost, exec_time_ms):
        """Utilidad para que todos devuelvan el mismo formato al Parser"""
        return {
            "data": data,
            "disk_accesses": io_cost,
            "execution_time_ms": exec_time_ms
        }

    @abstractmethod
    def add(self, key, record) -> dict:
        pass

    @abstractmethod
    def search(self, key) -> dict:
        pass

    @abstractmethod
    def remove(self, key) -> dict:
        pass

    @abstractmethod
    def range_search(self, begin_key, end_key) -> dict:
        pass
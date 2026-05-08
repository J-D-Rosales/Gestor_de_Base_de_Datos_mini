"""
Transaction Manager: simulación didáctica de dos transacciones concurrentes.

Por ahora NO implementa control de concurrencia (locks, 2PL, MVCC, etc.).
Su único objetivo es:

  1. Definir transacciones como una secuencia de operaciones (AST del parser).
  2. Intercalar sus operaciones (scheduling round-robin) para emular ejecución
     concurrente.
  3. Producir un log con BEGIN, cada operación con su resultado, y COMMIT.

La idea es que, cuando se agregue un lock manager, este módulo sea el punto
donde se solicitan/liberen locks antes de delegar al Executor.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from itertools import zip_longest

from executor import Executor


# ──────────────────────────────────────────────────────────────────────────
# Modelo
# ──────────────────────────────────────────────────────────────────────────
@dataclass
class Transaction:
    tx_id: str
    operations: list[dict] = field(default_factory=list)


@dataclass
class LogEntry:
    timestamp: float
    tx_id: str
    event: str            # BEGIN | OP | COMMIT
    detail: str = ""

    def render(self, t0: float) -> str:
        rel = (self.timestamp - t0) * 1000
        return f"[t+{rel:7.2f} ms] {self.tx_id:>3} {self.event:<6} {self.detail}"


# ──────────────────────────────────────────────────────────────────────────
# Manager
# ──────────────────────────────────────────────────────────────────────────
class TransactionManager:
    """
    Interleaving determinístico (round-robin) de N transacciones sobre un
    Executor compartido. Sin manejo de conflictos.
    """

    def __init__(self, executor: Executor):
        self.executor = executor
        self.log: list[LogEntry] = []

    # ---- API -----------------------------------------------------------
    def run(self, transactions: list[Transaction]) -> list[LogEntry]:
        self.log = []
        t0 = time.perf_counter()

        # BEGIN de cada transacción
        for tx in transactions:
            self._emit(tx.tx_id, "BEGIN", f"({len(tx.operations)} operaciones)")

        # Intercalar: una operación por transacción por ronda
        for paso in zip_longest(*(tx.operations for tx in transactions)):
            for tx, op in zip(transactions, paso):
                if op is None:
                    continue
                self._exec_op(tx.tx_id, op)

        # COMMIT
        for tx in transactions:
            self._emit(tx.tx_id, "COMMIT")

        return self.log

    # ---- helpers -------------------------------------------------------
    def _exec_op(self, tx_id: str, op: dict):
        descripcion = self._describe(op)
        self._emit(tx_id, "OP", f"{descripcion}  ...")
        result = self.executor.execute(op)
        self._emit(tx_id, "OP", f"{descripcion}  -> {self._summary(result)}")

    def _emit(self, tx_id: str, event: str, detail: str = ""):
        self.log.append(LogEntry(time.perf_counter(), tx_id, event, detail))

    @staticmethod
    def _describe(op: dict) -> str:
        t = op.get("tipo_operacion", "?")
        tabla = op.get("tabla", "")
        if t == "INSERT":
            return f"INSERT INTO {tabla} VALUES{tuple(op['valores_a_insertar'])}"
        if t == "SELECT_POINT":
            c = op["condicion"]
            return f"SELECT * FROM {tabla} WHERE {c['columna_where']} = {c['valor']!r}"
        if t == "SELECT_RANGE":
            c = op["condicion"]
            return (f"SELECT * FROM {tabla} WHERE {c['columna_where']} "
                    f"BETWEEN {c['valor_inicio']!r} AND {c['valor_fin']!r}")
        if t == "DELETE":
            c = op["condicion"]
            return f"DELETE FROM {tabla} WHERE {c['columna_where']} = {c['valor']!r}"
        if t == "CREATE_TABLE":
            return f"CREATE TABLE {tabla}"
        return f"{t} {tabla}"

    @staticmethod
    def _summary(result) -> str:
        if not isinstance(result, dict):
            return str(result)
        if result.get("status") == "error":
            return f"ERROR: {result.get('msg')}"
        op = result.get("op")
        if op == "INSERT":
            return f"OK rid={result.get('rid')}"
        if op == "SELECT_POINT":
            filas = result.get("filas", [])
            return f"OK {len(filas)} fila(s) {filas}"
        if op == "SELECT_RANGE":
            return f"OK {len(result.get('filas', []))} fila(s)"
        if op == "DELETE":
            return f"OK borrados={result.get('borrados')}"
        if op == "CREATE_TABLE":
            return f"OK formato={result.get('formato')}"
        return "OK"

    # ---- output --------------------------------------------------------
    def print_log(self):
        if not self.log:
            print("(log vacío)")
            return
        t0 = self.log[0].timestamp
        print("-" * 78)
        print(f"{'TIEMPO':>13}  {'TX':>3} {'EVENTO':<6} DETALLE")
        print("-" * 78)
        for e in self.log:
            print(e.render(t0))
        print("-" * 78)


# ──────────────────────────────────────────────────────────────────────────
# Transacciones predefinidas para la prueba
# ──────────────────────────────────────────────────────────────────────────
def _setup_table(executor: Executor, tabla: str = "alumnos"):
    """Crea la tabla y carga unos pocos registros base antes del test."""
    executor.execute({
        "tipo_operacion": "CREATE_TABLE",
        "tabla": tabla,
        "csv_path": None,
        "columnas": [
            {"nombre": "id",     "tipo": "INT",     "indice": "BTREE", "rtree_cols": None},
            {"nombre": "nombre", "tipo": "VARCHAR", "indice": None,    "rtree_cols": None},
            {"nombre": "nota",   "tipo": "FLOAT",   "indice": None,    "rtree_cols": None},
        ],
    })
    for fila in [(10, "Ana", 18.5), (20, "Beto", 14.0), (30, "Cris", 16.0)]:
        executor.execute({
            "tipo_operacion": "INSERT",
            "tabla": tabla,
            "valores_a_insertar": list(fila),
        })


def transacciones_demo() -> list[Transaction]:
    """Dos transacciones predefinidas que tocan la tabla 'alumnos'."""
    t1 = Transaction(tx_id="T1", operations=[
        {"tipo_operacion": "INSERT",
         "tabla": "alumnos",
         "valores_a_insertar": [40, "Dora", 17.0]},
        {"tipo_operacion": "SELECT_POINT",
         "tabla": "alumnos",
         "condicion": {"columna_where": "id", "operador": "=", "valor": 10}},
        {"tipo_operacion": "DELETE",
         "tabla": "alumnos",
         "condicion": {"columna_where": "id", "operador": "=", "valor": 20}},
    ])

    t2 = Transaction(tx_id="T2", operations=[
        {"tipo_operacion": "INSERT",
         "tabla": "alumnos",
         "valores_a_insertar": [50, "Eli", 19.0]},
        {"tipo_operacion": "SELECT_POINT",
         "tabla": "alumnos",
         "condicion": {"columna_where": "id", "operador": "=", "valor": 50}},
        {"tipo_operacion": "SELECT_POINT",
         "tabla": "alumnos",
         "condicion": {"columna_where": "id", "operador": "=", "valor": 30}},
    ])
    return [t1, t2]


if __name__ == "__main__":
    ex = Executor()
    _setup_table(ex)
    tm = TransactionManager(ex)
    tm.run(transacciones_demo())
    tm.print_log()

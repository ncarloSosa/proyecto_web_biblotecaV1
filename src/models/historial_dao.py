# src/models/historial_dao.py
from .db import query_all, query_one, execute, first_existing_column

TABLE = "HISTORIAL"

def _col(cands):
    try:
        return first_existing_column(TABLE, cands)
    except Exception:
        return None

def _pk():
    try:
        return first_existing_column(TABLE, ["ID_HISTORIAL", "ID"])
    except Exception:
        return "ROWID"

def listar():
    pk = _pk()
    fec = _col(["FECHA", "FECHA_MOVIMIENTO", "FEC_REGISTRO"])
    acc = _col(["ACCION", "MOVIMIENTO", "ACTIVIDAD"])
    usr = _col(["USUARIO_ID_USUARIO", "ID_USUARIO", "USUARIO_ID"])
    lib = _col(["LIBRO_ID_LIBRO", "ID_LIBRO", "LIBRO_ID"])

    cols = [f"{pk} AS ID_HISTORIAL"]
    if fec: cols.append(f"{fec} AS FECHA")
    if acc: cols.append(f"{acc} AS ACCION")
    if usr: cols.append(f"{usr} AS ID_USUARIO")
    if lib: cols.append(f"{lib} AS ID_LIBRO")

    sql = f"SELECT {', '.join(cols)} FROM {TABLE} ORDER BY {('FECHA' if fec else 'ID_HISTORIAL')} DESC"
    return query_all(sql)

def crear(data: dict):
    pk = _pk()
    fec_col = _col(["FECHA", "FECHA_MOVIMIENTO", "FEC_REGISTRO"])
    acc_col = _col(["ACCION", "MOVIMIENTO", "ACTIVIDAD"])
    usr_col = _col(["USUARIO_ID_USUARIO", "ID_USUARIO", "USUARIO_ID"])
    lib_col = _col(["LIBRO_ID_LIBRO", "ID_LIBRO", "LIBRO_ID"])

    columns, values, binds = [], [], {}

    # PK con secuencia si existe
    seq = query_one(
        "SELECT sequence_name FROM user_sequences "
        "WHERE UPPER(sequence_name) LIKE 'HISTORIAL%SEQ' FETCH FIRST 1 ROWS ONLY"
    )
    if pk.upper() != "ROWID":
        columns.append(pk)
        if seq:
            values.append(f"{seq['SEQUENCE_NAME']}.NEXTVAL")
        else:
            new_id = (query_one(f"SELECT NVL(MAX({pk}),0)+1 ID FROM {TABLE}") or {"ID": 1})["ID"]
            values.append(":_pk")
            binds["_pk"] = new_id

    def add(col, ph, val):
        if not col:
            return
        columns.append(col); values.append(f":{ph}"); binds[ph] = val

    add(fec_col, "fec", data.get("FECHA"))
    add(acc_col, "acc", data.get("ACCION") or data.get("MOVIMIENTO"))
    add(usr_col, "usr", data.get("ID_USUARIO") or data.get("USUARIO_ID_USUARIO"))
    add(lib_col, "lib", data.get("ID_LIBRO") or data.get("LIBRO_ID_LIBRO"))

    sql = f"INSERT INTO {TABLE} ({', '.join(columns)}) VALUES ({', '.join(values)})"
    execute(sql, binds)

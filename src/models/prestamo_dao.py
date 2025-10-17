# src/models/prestamo_dao.py
from .db import query_all, query_one, execute, first_existing_column

TABLE = "PRESTAMO"

def _col(candidates):
    # devuelve el primer nombre de columna que exista en PRESTAMO
    return first_existing_column(TABLE, candidates)

def _pk():
    try:
        return first_existing_column(TABLE, ["ID_PRESTAMO", "ID"])
    except Exception:
        # si no hay PK definida usamos ROWID
        return "ROWID"

def _find_sequence_like(prefix="PRESTAMO"):
    row = query_one(
        "SELECT sequence_name FROM user_sequences "
        "WHERE UPPER(sequence_name) LIKE :p FETCH FIRST 1 ROWS ONLY",
        {"p": f"{prefix.upper()}%SEQ"}
    )
    return row["SEQUENCE_NAME"] if row else None

def listar():
    pk = _pk()
    fpr = _col(["FECHA_PRESTAMO", "FECHA", "FECHA_INICIO"])
    fca = _col(["FECHA_CADUCIDAD", "FECHA_DEVOLUCION", "FECHA_ENTREGA", "FECHA_FIN", "FECHA_VENCIMIENTO"])
    est = _col(["ESTADO", "STATUS"])
    efi = _col(["ESTADO_FISICO", "CONDICION", "CONDICION_FISICA"])
    lib = _col(["LIBRO_ID_LIBRO", "ID_LIBRO", "LIBRO_ID"])
    usr = _col(["USUARIO_ID_USUARIO", "ID_USUARIO", "USUARIO_ID"])

    cols = [f"{pk} AS ID_PRESTAMO"]
    if fpr: cols.append(f"{fpr} AS FECHA_PRESTAMO")
    if fca: cols.append(f"{fca} AS FECHA_CADUCIDAD")
    if est: cols.append(f"{est} AS ESTADO")
    if efi: cols.append(f"{efi} AS ESTADO_FISICO")
    if lib: cols.append(f"{lib} AS ID_LIBRO")
    if usr: cols.append(f"{usr} AS ID_USUARIO")

    sql = f"SELECT {', '.join(cols)} FROM {TABLE} ORDER BY {cols[1].split(' AS ')[0]} DESC"
    return query_all(sql)

def obtener(id_prestamo):
    pk = _pk()
    return query_one(f"SELECT * FROM {TABLE} WHERE {pk} = :id", {"id": id_prestamo})

def crear(data: dict):
    pk = _pk()
    fpr_col = _col(["FECHA_PRESTAMO", "FECHA", "FECHA_INICIO"])
    fca_col = _col(["FECHA_CADUCIDAD", "FECHA_DEVOLUCION", "FECHA_ENTREGA", "FECHA_FIN", "FECHA_VENCIMIENTO"])
    est_col = _col(["ESTADO", "STATUS"])
    efi_col = _col(["ESTADO_FISICO", "CONDICION", "CONDICION_FISICA"])
    lib_col = _col(["LIBRO_ID_LIBRO", "ID_LIBRO", "LIBRO_ID"])
    usr_col = _col(["USUARIO_ID_USUARIO", "ID_USUARIO", "USUARIO_ID"])

    # Valores que vienen del formulario (tolerantes a nombres alternos)
    fpr_val = data.get("FECHA_PRESTAMO") or data.get("FECHA") or data.get("FECHA_PRESTADO")
    fca_val = data.get("FECHA_CADUCIDAD") or data.get("FECHA_DEVOLUCION")
    est_val = data.get("ESTADO")
    efi_val = data.get("ESTADO_FISICO")
    lib_val = data.get("ID_LIBRO") or data.get("LIBRO_ID_LIBRO")
    usr_val = data.get("ID_USUARIO") or data.get("USUARIO_ID_USUARIO")

    columns, values, binds = [], [], {}

    # PK: secuencia si existe; si no, MAX+1 como último recurso
    seq = _find_sequence_like("PRESTAMO")
    if pk.upper() != "ROWID":
        columns.append(pk)
        if seq:
            values.append(f"{seq}.NEXTVAL")
        else:
            new_id = (query_one(f"SELECT NVL(MAX({pk}),0)+1 AS ID FROM {TABLE}") or {"ID": 1})["ID"]
            values.append(":_pk")
            binds["_pk"] = new_id

    def add(col, placeholder, val):
        if not col:
            return
        if col in columns:  # evita columnas duplicadas
            return
        columns.append(col)
        values.append(f":{placeholder}")
        binds[placeholder] = val

    add(fpr_col, "fpr", fpr_val)
    add(fca_col, "fca", fca_val)
    add(est_col, "est", est_val)
    add(efi_col, "efi", efi_val)
    add(lib_col, "lib", lib_val)
    add(usr_col, "usr", usr_val)

    sql = f"INSERT INTO {TABLE} ({', '.join(columns)}) VALUES ({', '.join(values)})"
    execute(sql, binds)

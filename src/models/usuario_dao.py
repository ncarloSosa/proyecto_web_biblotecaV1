from .db import query_all, query_one, execute

def obtener(id_usuario: int):
    sql = """
    SELECT ID_USUARIO,
           NOMBRE,
           DIRECCION,
           TELEFONO,
           DPI,
           SEXO,
           FECHA_CREACION,
           CONTRASENA
      FROM USUARIO
     WHERE ID_USUARIO = :ID
    """
    return query_one(sql, {"ID": id_usuario})

def buscar_por_nombre(nombre: str):
    sql = """
    SELECT ID_USUARIO,
           NOMBRE,
           CONTRASENA
      FROM USUARIO
     WHERE NOMBRE = :NOMBRE
    """
    return query_one(sql, {"NOMBRE": nombre})

def listar():
    sql = """
    SELECT ID_USUARIO,
           NOMBRE,
           DIRECCION,
           TELEFONO,
           DPI,
           SEXO,
           FECHA_CREACION,
           CONTRASENA
      FROM USUARIO
     ORDER BY ID_USUARIO DESC
    """
    return query_all(sql)

def crear(data: dict):
    sql = """
    INSERT INTO USUARIO
      (ID_USUARIO, NOMBRE, DIRECCION, TELEFONO, DPI, SEXO, FECHA_CREACION, CONTRASENA)
    VALUES
      ((SELECT NVL(MAX(ID_USUARIO),0)+1 FROM USUARIO),
       :NOMBRE, :DIRECCION, :TELEFONO, :DPI, :SEXO,
       TO_DATE(:FECHA_CREACION,'YYYY-MM-DD'), :CONTRASENA)
    """
    execute(sql, data)

def actualizar(id_usuario: int, data: dict):
    sql = """
    UPDATE USUARIO
       SET NOMBRE = :NOMBRE,
           DIRECCION = :DIRECCION,
           TELEFONO = :TELEFONO,
           DPI = :DPI,
           SEXO = :SEXO,
           FECHA_CREACION = TO_DATE(:FECHA_CREACION,'YYYY-MM-DD'),
           CONTRASENA = :CONTRASENA
     WHERE ID_USUARIO = :ID
    """
    execute(sql, dict(data, ID=id_usuario))

def eliminar(id_usuario: int):
    execute("DELETE FROM USUARIO WHERE ID_USUARIO = :ID", {"ID": id_usuario})

from file import File as HFile, Row

class Table:
    """
    Clase Table que representa una tabla con familias de columnas y filas.

    Attributes:
        name (str): Nombre de la tabla.
        family_columns (dict): Diccionario que contiene las familias de columnas y sus respectivas columnas.
        h_files (list): Lista de archivos HFile que contienen las filas de la tabla.
        enabled (bool): Indicador de si la tabla está habilitada o no.

    Methods:
        disable(): Deshabilita la tabla.
        enable(): Habilita la tabla.
        is_enabled(): Verifica si la tabla está habilitada.
        add_family(name): Añade una familia de columnas a la tabla.
        delete_family(name): Elimina una familia de columnas de la tabla.
        add_column(family, column): Añade una columna a una familia de columnas específica.
        change_name(name): Cambia el nombre de la tabla.
        put(table_name, row_key, column_family, column_name, value, timestamp): Inserta un valor en la tabla.
        get(row_key, column_family, column, version): Obtiene un valor de la tabla.
        delete(row_key, column_family, column, timestamp): Elimina un valor de la tabla.
        truncate(): Vacía la tabla.
        count(): Cuenta el número de filas en la tabla.
        scan(start_row, end_row, limit): Escanea las filas de la tabla.
        describe(): Describe la tabla.
    """

    def __init__(self, name, groups):
        """
        Constructor para la clase Table.

        Args:
            name (str): Nombre de la tabla.
            groups (list): Lista de familias de columnas.

        Inicializa una nueva tabla con el nombre dado y las familias de columnas proporcionadas.
        """
        self.name = name
        self.family_columns = {}  # Diccionario que almacena las familias de columnas y sus columnas
        self.h_files = []  # Lista de archivos HFile
        self.enabled = True  # Indicador de si la tabla está habilitada
        for fam in groups:
            self.family_columns[fam] = []  # Añade cada familia de columnas a la tabla

    def disable(self):
        """
        Deshabilita la tabla.
        """
        self.enabled = False

    def enable(self):
        """
        Habilita la tabla.
        """
        self.enabled = True

    def is_enabled(self):
        """
        Verifica si la tabla está habilitada.

        Returns:
            bool: True si la tabla está habilitada, False en caso contrario.
        """
        return self.enabled

    def add_family(self, name):
        """
        Añade una familia de columnas a la tabla.

        Args:
            name (str): Nombre de la familia de columnas.

        Returns:
            bool: True si la familia se añadió correctamente, False en caso contrario.
        """
        if not self.enabled:
            return False
        if name in self.family_columns.keys():
            return False
        self.family_columns[name] = []  # Añade la nueva familia de columnas
        return True

    def delete_family(self, name):
        """
        Elimina una familia de columnas de la tabla.

        Args:
            name (str): Nombre de la familia de columnas.

        Returns:
            bool: True si la familia se eliminó correctamente, False en caso contrario.
        """
        if not self.enabled:
            return False
        if name not in self.family_columns.keys():
            return False
        del self.family_columns[name]  # Elimina la familia de columnas del diccionario
        return True

    def add_column(self, family, column):
        """
        Añade una columna a una familia de columnas específica.

        Args:
            family (str): Nombre de la familia de columnas.
            column (str): Nombre de la columna.

        Returns:
            bool: True si la columna se añadió correctamente, False en caso contrario.
        """
        if not self.enabled:
            return False
        if family not in self.family_columns.keys():
            return False
        if column in self.family_columns[family]:
            return False
        self.family_columns[family].append(column)  # Añade la columna a la familia correspondiente
        return True

    def change_name(self, name):
        """
        Cambia el nombre de la tabla.

        Args:
            name (str): Nuevo nombre de la tabla.

        Returns:
            bool: True si el nombre se cambió correctamente, False en caso contrario.
        """
        if not self.enabled:
            return False
        self.name = name

    def put(self, row_key, column_family, column_name, value):
        """
        Inserta un valor en la tabla.

        Args:
            table_name (str): Nombre de la tabla.
            row_key (str): Clave de la fila.
            column_family (str): Nombre de la familia de columnas.
            column_name (str): Nombre de la columna.
            value (any): Valor a insertar.
            timestamp (float, optional): Timestamp para el valor. Si no se proporciona, se usa el tiempo actual.

        Returns:
            bool: True si el valor se insertó correctamente, False en caso contrario.
        """
        if not self.enabled:
            return False
        if column_family not in self.family_columns.keys():
            return False
        if column_name not in self.family_columns[column_family]:
            return False

        if not self.h_files:
            row_t = Row(row_key, f'{column_family}:{column_name}', value)
            h_file_t = HFile([row_t], column_family)
            self.h_files = [h_file_t]
            return True
        else:
            for hf in self.h_files:
                if column_family == hf.column_family:
                    hf.create_row(row_key, f'{column_family}:{column_name}', value)
                    return True
                else:
                    return False

    def get(self, row_key, column_family, column, version=1):
        """
        Obtiene un valor de la tabla.

        Args:
            row_key (str): Clave de la fila.
            column_family (str): Nombre de la familia de columnas.
            column (str): Nombre de la columna.
            version (int, optional): Versión del valor. Por defecto es 1.

        Returns:
            any: Valor obtenido de la tabla, o None si no se encontró.
        """
        if not self.enabled:
            return None
        if column_family not in self.family_columns.keys():
            return None
        if column not in self.family_columns[column_family]:
            return None
        for h_file in self.h_files:
            rows = h_file.get(row_key, column_family, column, version)
            if len(rows) != 0:
                return rows
        return None

    def delete(self, row_key, column_family=None, column=None, timestamp=None):
        """
        Elimina un valor de la tabla.

        Args:
            row_key (str): Clave de la fila.
            column_family (str, optional): Nombre de la familia de columnas.
            column (str, optional): Nombre de la columna.
            timestamp (float, optional): Timestamp del valor.

        Returns:
            int: Número de filas eliminadas.
        """
        deleted_rows = 0
        if not self.enabled:
            return deleted_rows
        if column_family is not None and column_family not in self.family_columns.keys():
            return deleted_rows
        if column is not None and column not in self.family_columns[column_family]:
            return deleted_rows
        for h_file in self.h_files:
            deleted_rows += h_file.delete(row_key, column_family, column, timestamp)
        return deleted_rows

    def truncate(self):
        """
        Vacía la tabla.

        Returns:
            bool: True si la operación se realizó correctamente.
        """
        self.h_files = []
        return True

    def count(self):
        """
        Cuenta el número de filas en la tabla.

        Returns:
            int: Número de filas en la tabla.
        """
        count = 0
        for h_file in self.h_files:
            for r in h_file.rows:
                if r.enabled:
                    count += 1
        return count

    def scan(self, start_row=None, end_row=None, limit=None):
        """
        Escanea las filas de la tabla.

        Args:
            start_row (str, optional): Clave de la fila inicial.
            end_row (str, optional): Clave de la fila final.
            limit (int, optional): Límite de filas a escanear.

        Returns:
            list: Lista de filas escaneadas.
        """
        if not self.enabled:
            return False

        rows = []
        if not start_row and not end_row:
            for h_file in self.h_files:
                count = 0
                old_key = None
                for row in h_file.rows:
                    if old_key and old_key != row.key:
                        count += 1
                    if limit and count == limit:
                        break
                    old_key = row.key
                    rows.append(row)

        if start_row and end_row:
            for h_file in self.h_files:
                for row in h_file.rows:
                    if row.key >= start_row and row.key < end_row:
                        rows.append(row)
        return rows

    def describe(self):
        """
        Describe la tabla, mostrando su estado, nombre y familias de columnas.
        """
        enabled = "ENABLED" if self.enabled else "DISABLED"
        print("Table " + self.name + " is " + enabled)
        print(self.name)
        print("COLUMN FAMILIES DESCRIPTION")
        for column_family in self.family_columns.keys():
            print("{NAME => '" + column_family + "' VERSIONS => '1'}")
        print(str(len(self.family_columns.keys())) + " row(s)")

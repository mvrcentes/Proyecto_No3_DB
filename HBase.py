from tabla import Tabla

class HBase:
    def __init__(self) -> None:
        """
        Constructor para inicializar HBase con un diccionario vacío.
        Este diccionario simulará las tablas, facilitando su manejo.
        """
        self.tables = {}  # Inicializa el diccionario vacío que contendrá las tablas

    def create(self, name, familia_columnas):
        """
        Crea una nueva tabla con el nombre especificado y las familias de columnas dadas.

        Args:
            name (str): El nombre de la tabla a crear.
            familia_columnas (list): Una lista de nombres de las familias de columnas.

        Returns:
            bool: True si la tabla se crea correctamente, False si la tabla ya existe.
        """
        if name not in self.tables.keys():  # Verifica si la tabla no existe
            table = Tabla(name, familia_columnas)  # Crea una instancia de la clase Tabla
            self.tables[name] = table  # Añade la tabla al diccionario de tablas
            return True  # Devuelve True si la tabla se ha creado correctamente
        return False  # Devuelve False si la tabla ya existe

    def list(self):
        """
        Lista los nombres de todas las tablas existentes.

        Returns:
            dict_keys: Una vista de los nombres de las tablas.
        """
        return self.tables.keys()  # Devuelve las claves del diccionario de tablas

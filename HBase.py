from tabla import Tabla

class HBase:
    def __init__(self) -> None:
        """
        Constructor para inicializar HBase con un diccionario vacío.
        Este diccionario simulará las tablas, facilitando su manejo.
        """
        self.tables = {}  # Inicializa el diccionario vacío que contendrá las tablas

    def Create(self, name, familia_columnas):
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

    def List(self):
        """
        Lista los nombres de todas las tablas existentes.

        Returns:
            dict_keys: Una vista de los nombres de las tablas.
        """
        return self.tables.keys()  # Devuelve las claves del diccionario de tablas

    def Enable(self, name):
        if name in self.tables.keys():
            self.tables[name].enable()
            return True
        return False

    def Disable(self, name):
        if name in self.tables.keys():
            self.tables[name].disable()
            return True
        return False
    
    def Is_enable(self,name):
        if name in self.tables.keys():
            return self.tables[name].is_enable()
        return False
    
    def Alter_table_add(self, name, familia_columnas):
        if name in self.tables.keys():
            return self.tables[name].add_family(familia_columnas)
        return False
    
    def Alter_table_delete(self,name,familia_columnas):
        if name in self.tables.keys():
            return self.tables[name].delete_family(familia_columnas)
        return False
        
    def Alter_table_add_column(self, name, familia_columnas, columna):
        if name in self.tables.keys():
            return self.tables[name].add_column(familia_columnas, columna)
        return False
    
    def Alter_table_name(self,table , nuevo_nombre):
        nombre_viejo = table in self.tables.keys()
        nombre_repetido = nuevo_nombre in self.tables.keys()

        if nombre_viejo and not nombre_repetido:
            tabla_anterior = self.tables[table]
            del self.tables[table]
            tabla_anterior.change_name(nuevo_nombre)
            self.tables[nuevo_nombre] = tabla_anterior
            return True
        return False
    
    def Describe(self, nombre_tabla):
        if nombre_tabla not in self.tables.keys():
            return False
        self.tables[nombre_tabla].describe()
        return True
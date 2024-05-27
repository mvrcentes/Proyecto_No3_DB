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
		"""
		Habilita la tabla con el nombre dado si existe.
		
		Parámetros:
		name (str): El nombre de la tabla a habilitar.
		
		Retorna:
		bool: True si la tabla fue habilitada exitosamente, False en caso contrario.
		"""
		if name in self.tables.keys():  # Verifica si la tabla existe
			self.tables[name].enable()  # Habilita la tabla
			return True  # Retorna True indicando éxito
		return False  # Retorna False si la tabla no existe

	def Disable(self, name):
		"""
		Deshabilita la tabla con el nombre dado si existe.
		
		Parámetros:
		name (str): El nombre de la tabla a deshabilitar.
		
		Retorna:
		bool: True si la tabla fue deshabilitada exitosamente, False en caso contrario.
		"""
		if name in self.tables.keys():  # Verifica si la tabla existe
			self.tables[name].disable()  # Deshabilita la tabla
			return True  # Retorna True indicando éxito
		return False  # Retorna False si la tabla no existe
	
	def Is_enable(self, name):
		"""
		Verifica si la tabla con el nombre dado está habilitada.
		
		Parámetros:
		name (str): El nombre de la tabla a verificar.
		
		Retorna:
		bool: True si la tabla está habilitada, False en caso contrario.
		"""
		if name in self.tables.keys():  # Verifica si la tabla existe
			return self.tables[name].is_enable()  # Retorna el estado de habilitación
		return False  # Retorna False si la tabla no existe
	
	def Alter_table_add(self, name, familia_columnas):
		"""
		Agrega una familia de columnas a la tabla con el nombre dado si existe.
		
		Parámetros:
		name (str): El nombre de la tabla a modificar.
		familia_columnas (str): El nombre de la familia de columnas a agregar.
		
		Retorna:
		bool: True si la familia de columnas fue agregada exitosamente, False en caso contrario.
		"""
		if name in self.tables.keys():  # Verifica si la tabla existe
			return self.tables[name].add_family(familia_columnas)  # Agrega la familia de columnas
		return False  # Retorna False si la tabla no existe
	
	def Alter_table_delete(self, name, familia_columnas):
		"""
		Elimina una familia de columnas de la tabla con el nombre dado si existe.
		
		Parámetros:
		name (str): El nombre de la tabla a modificar.
		familia_columnas (str): El nombre de la familia de columnas a eliminar.
		
		Retorna:
		bool: True si la familia de columnas fue eliminada exitosamente, False en caso contrario.
		"""
		if name in self.tables.keys():  # Verifica si la tabla existe
			return self.tables[name].delete_family(familia_columnas)  # Elimina la familia de columnas
		return False  # Retorna False si la tabla no existe
		
	def Alter_table_add_column(self, name, familia_columnas, columna):
		"""
		Agrega una columna a una familia de columnas especificada en la tabla con el nombre dado si existe.
		
		Parámetros:
		name (str): El nombre de la tabla a modificar.
		familia_columnas (str): El nombre de la familia de columnas a modificar.
		columna (str): El nombre de la columna a agregar.
		
		Retorna:
		bool: True si la columna fue agregada exitosamente, False en caso contrario.
		"""
		if name in self.tables.keys():  # Verifica si la tabla existe
			return self.tables[name].add_column(familia_columnas, columna)  # Agrega la columna
		return False  # Retorna False si la tabla no existe
	
	def Alter_table_name(self, table, nuevo_nombre):
		"""
		Renombra una tabla si existe y el nuevo nombre no está ya en uso.
		
		Parámetros:
		table (str): El nombre actual de la tabla a renombrar.
		nuevo_nombre (str): El nuevo nombre para la tabla.
		
		Retorna:
		bool: True si la tabla fue renombrada exitosamente, False en caso contrario.
		"""
		nombre_viejo = table in self.tables.keys()  # Verifica si la tabla actual existe
		nombre_repetido = nuevo_nombre in self.tables.keys()  # Verifica si el nuevo nombre ya está en uso

		if nombre_viejo and not nombre_repetido:
			tabla_anterior = self.tables[table]  # Obtiene la tabla actual
			del self.tables[table]  # Elimina la entrada de la tabla actual
			tabla_anterior.change_name(nuevo_nombre)  # Cambia el nombre de la tabla
			self.tables[nuevo_nombre] = tabla_anterior  # Añade la tabla con el nuevo nombre
			return True  # Retorna True indicando éxito
		return False  # Retorna False si el renombrado no es posible
	
	def Delete_table(self, name):
		"""
		Elimina la tabla con el nombre dado si existe y está deshabilitada.
		
		Parámetros:
		name (str): El nombre de la tabla a eliminar.
		
		Retorna:
		bool: True si la tabla fue eliminada exitosamente, False en caso contrario.
		"""
		if name in self.tables.keys() and not self.tables[name].is_enable():  # Verifica si la tabla existe y está deshabilitada
			del self.tables[name]  # Elimina la tabla
			return True  # Retorna True indicando éxito
		return False  # Retorna False si la tabla no existe o está habilitada

	def Delete_all_table(self):
		"""
		Elimina todas las tablas.
		
		Retorna:
		bool: True indicando que todas las tablas fueron eliminadas.
		"""
		self.tables = {}  # Elimina todas las tablas
		return True  # Retorna True indicando éxito

	def Describe(self, nombre_tabla):
		"""
		Describe la tabla con el nombre dado si existe.
		
		Parámetros:
		nombre_tabla (str): El nombre de la tabla a describir.
		
		Retorna:
		bool: True si la descripción se realizó exitosamente, False en caso contrario.
		"""
		if nombre_tabla not in self.tables.keys():  # Verifica si la tabla no existe
			return False  # Retorna False si la tabla no existe
		self.tables[nombre_tabla].describe()  # Describe la tabla
		return True  # Retorna True indicando éxito

	def Get(self, nombre_tabla, row_key, familia_columnas, version):
		if nombre_tabla in self.tables.keys():
			rows = []
			for colmna in familia_columnas:
				cf, col = colmna.split(':')
				filas_encontradas = self.tables[nombre_tabla].get(row_key, cf, col, version)
				if filas_encontradas:
					rows.append(filas_encontradas)
			return rows
		return None
	
	def Scan(self, nombre_tabla, familia_columnas, version):
		if nombre_tabla in self.tables.keys():
			rows = []
			for colmna in familia_columnas:
				cf, col = colmna.split(':')
				filas_encontradas = self.tables[nombre_tabla].scan(cf, col, version)
				if filas_encontradas:
					rows.append(filas_encontradas)
			return rows
		return None
	
	def Count(self, nombre_tabla):
		if nombre_tabla in self.tables.keys():
			return self.tables[nombre_tabla].count()
		return None
	
	def Put(self, nombre_tabla, row_key, familia_columnas, valores):
		if nombre_tabla in self.tables.keys():
			for colmna in familia_columnas:
				cf, col = colmna.split(':')
				self.tables[nombre_tabla].put(row_key, cf, col, valores)
			return True
		return False
	
	def Truncate(self, nombre_tabla):
		if nombre_tabla in self.tables.keys():
			self.Disable(nombre_tabla)
			self.tables[nombre_tabla].truncate()
			self.Enable(nombre_tabla)
			return True
		return False